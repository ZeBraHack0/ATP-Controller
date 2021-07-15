# Parameter server architecture

## Motivation

- 实际训练数据量可以达到1TB到1PB之间
- 实际模型大小可以达到$10^9$ to $10^{12}$

## Architecture overview

- Abstract architecture

![img](https://imgconvert.csdnimg.cn/aHR0cHM6Ly9tbWJpei5xcGljLmNuL21tYml6X3BuZy9XQzBzWG9rQUg3RFp5RkJSbTVPY3A1cVdlS2VITTR4Q2hvSUZpYnRnQ2JzeTN5N1A5cWt1SW0zZTM1VEs4UVdhaFJudFVTS1U4aWNwQXlLek5ySk9aVVNBLzY0MA?x-oss-process=image/format,png)

- Physical architecture

![img](https://imgconvert.csdnimg.cn/aHR0cHM6Ly9tbWJpei5xcGljLmNuL21tYml6X3BuZy9XQzBzWG9rQUg3RFp5RkJSbTVPY3A1cVdlS2VITTR4Q1FhRkVhUWNMQnFsbjNEemtGNTVpYVdHaWF2TzRpY1A2S3NxTFIzckx2NW5NOHZ1czBQaEQ2eTNsUS82NDA?x-oss-process=image/format,png)

​		每个server node负责维护一部分参数，每个worker node只与server node通讯

- 模型参数的存储

  按照(feature_ID, weight_vector)形式存储的key-value store

## Consistency and Efficiency

- 异步更新：允许worker仅使用当前时刻的最新模型进行新一轮的训练（即前几轮的梯度计算结果可能尚未被同步到ps端的模型中），此时由于不需要在每个iteration中进行同步阻断（所有协同者均完成该轮训练才能进入下一轮），可以提高训练效率，但是使用不一致的模型进行分布式训练可能会影响收敛速度
- 任务依赖模式：
  - sequential 完全同步
  - eventual 完全异步
  - bounded delay 同步与异步的trade-off，只有T轮之前的任务全部完成才能进入下一轮

## Parameter Assignment

server manager 按照一致性哈希来为每个key-value pair（模型参数）分配负责的服务器

## Vector Clock

- Initially, each parameter should record the timestamps of all worker nodes, which called vector clock taking O(mn) space complexity
- Fortunately, many parameters hare the same timestamp as a result of the range-based communication pattern of the parameter server. Therefore, they can be compressed into a single range vector clock, i.e., $vc_i(R) = t$
  means for any key $k \in R, vc_i(k) = t$



# ATP Background

## Motivation

- Performance bottleneck in distributed training is increasingly shifting from compute to communication because of: 
  - Advances in GPUs and other compute accelerators
  - The ratio of communication to computation in the workload itself has shifted: communication can begin earlier than when computation ends
  - More than 50% training time for 10Gbps bandwidth and 23% for 100Gbps
  - Severe communication bottle when GPUs become faster

## Architecture

- 在ATP下每个训练job最多支持32个rack，每个rack32个worker共同计算（双层32位bitmap）
- ATP只考虑数据划分的场景，每个worker持有完整的模型参数
- 在每个iteration中，worker计算梯度后按照顺序发送，接收返回的总梯度值并更新模型

## Aggregator

- 每个aggregator负责聚合31个integer（31 这个数是由交换机pipeline最长能处理的包头长度来决定的：12个stage约能容纳48个table，每个table负责处理一个integer，但是部分字段需要用于协议控制逻辑，因此实际上只保留了31个integer给数据段）
- Pool-based streaming aggregation: aggregator本身的数量不要求等于模型参数的数量：交换机本身不存储模型参数，只负责聚合梯度，因此在aggregator数超过一定程度以后（不同模型不同，约为数千个）带宽代替aggregator成为瓶颈——aggregator本身类似于pool，被各个参数占用后累加并返回，同时释放aggregator，因此可以循环使用（类似于滑动窗口），当aggregator数量达不到该瓶颈时，会出现聚合不完全、ps端流量增大的情况

## Tofino P4 Switch Background

- Programmable switches expose on-chip memory as stateful and
  stateless objects. 
  - Stateless objects, metadata, hold the state for each packet, and the switch releases this object when that packet is dropped or forwarded. 
  - Stateful objects, registers, hold state as long as the switch program is running. A register value can be read and written in the dataplane, but can only be accessed once, either for read or write or both, for each packet.
- Register memory can only be allocated when the switch program launches
- 由于网内聚合中交换机需要读取聚合的数据，因此这部分gradient并不能放在payload中，而是要放在header里，这导致了网内计算的数据包都是小包（payload里只保留少量end host端的控制信息）

## Contribution 

- reduce the network traffic
- eliminate the incast
- save the CPU cycles in the end hosts (tiny)

## Multiple-job, Multi-rack Challenge

- Only aggregate gradients in the ToR switches limit optimization of multi-rack tasks (in reverse, ATP aggregates gradients in the rack switch at the worker and the rack switch at the PS)
- Multiple-job aggregation propose high requirements on the switch resources, i.e., the best-effort uage of aggregators can improve the utilization of switch resources

## Protocol Design

- ATP implements reliable transfer, flow control and congestion control
- ATP does not implement in-order byte-stream and multiplexing abstractions
- CC: if there is queue buildup, say when queues were above a certain threshold when packet A2 was received, an ECN flag in A2 is set and carried over to A`2. This is copied to the parameter packet A0 in PS and received by the workers who adjust their windows.
- To avoid processing routine changes, only ToR switches are allowed for aggregation
- Aggregator Deallocation: The switch multicasts parameter packets back to the workers when it receives parameter packets from PS
- Packet size: 306B  (58B header + 248B gradient values)
- ATP converts floating-point numbers in gradients to 32-bit integers: multiplying the floating point number by a scaling factor ($10^8$)

## Reliability

- when the PS does not send parameter packets in sequence, a worker updates the received parameters but does not update expected sequence number. 
- When a worker receives three consecutive parameter packets other than the expected sequence number, it detects loss of the gradient fragment with the expected sequence number. In this case, ATP worker retransmits the missing fragment packet with the resend bit set; this indicates to switches that
  there may be a partial aggregation state in the switch.
- ATP does not try to do innetwork aggregation of resent gradients to avoid occupying aggregation
- To avoid job crashes lock aggregators, on every parameter packet, the switch checks the timeout value for the register specified by the parameter packet’s index.

# Congestion Control

- RTT measured from a worker sending a gradient packet to it receiving a parameter ACK packet will not work because it includes synchronization delay between workers
- both ECN and (rare) packet loss are used as the congestion signal
- AIMD strategy: 
  - The window size ATP starts at 200 packets, which at 300 bytes each packet is within bandwidth-delay product (~ 60KB) of a 100Gbps network. 
  - ATP increases window size by one MTU (1500 bytes or 5 packets) for each received parameter packet until it reaches a threshold, which is similar to slow start in TCP. 
  - Above the slow-start threshold, ATP increases window size by one MTU per window. 
  - When a worker detects congestion via ECN marking on a parameter ACK or three out-of-order ACKs, it halves the window, and updates the slow start threshold to the updated window

## Related Work

- SwitchML: no PS, synchronous update, aggregator allocation
- MLFabric: with PS, asynchronous update, aggregator allocation, per update scheduling