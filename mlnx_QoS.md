```go
func IptablesMatchOptions(rule *pb.Rule) []string {
    matchOptions := []string{}
    if rule.Cgroup_ID != "" { 
        matchOptions = append(matchOptions, "-m", "cgroup", "--cgroup", rule.Cgroup_ID)
    }

    if rule.Protocol == "" {
        // Default to match TCP
        matchOptions = append(matchOptions, "-p", "tcp")      
    } else {
        matchOptions = append(matchOptions, "-p", strings.ToLower(rule.Protocol))
    }

    if rule.SrcPortBegin != 0 {
        matchOptions = append(matchOptions, "--sport")
        if rule.SrcPortBegin >= rule.SrcPortEnd {
            matchOptions = append(matchOptions, strconv.Itoa(int(rule.SrcPortBegin)))
        } else {
            matchOptions = append(matchOptions, strconv.Itoa(int(rule.SrcPortBegin)) + ":" + strconv.Itoa(int(rule.SrcPortEnd)))
        }
    }

    if rule.DstPortBegin != 0 {
        matchOptions = append(matchOptions, "--dport")
        if rule.DstPortBegin >= rule.DstPortEnd {
            matchOptions = append(matchOptions, strconv.Itoa(int(rule.DstPortBegin)))
        } else {
            matchOptions = append(matchOptions, strconv.Itoa(int(rule.DstPortBegin)) + ":" + strconv.Itoa(int(rule.DstPortEnd)))
        }
    }

    /*
     * 1. Three methods to match multiple IP addresses in a single iptalbes rule:
     *     https://serverfault.com/questions/6989/iptables-multiple-source-ips-in-single-rule
     * 2. Note: the max capacity of a set in Ipset is 65536
     */
    if len(rule.Src_IP) > 0 {
        var ipsetSrcIP string = "Agent" + strconv.Itoa(int(rule.Service_ID)) + rule.Subservice + "Src"
        matchOptions = append(matchOptions, "-m", "set", "--match-set", ipsetSrcIP, "src")
    }
    if len(rule.Dst_IP) > 0 {
        var ipsetDstIP string = "Agent" + strconv.Itoa(int(rule.Service_ID)) + rule.Subservice + "Dst"
        matchOptions = append(matchOptions, "-m", "set", "--match-set", ipsetDstIP, "dst")
    }
    return matchOptions
}
```

```go
// Generate an iptables rule for certain service
// Op := -A or -D
func IptablesSetSkbMark(rule *pb.Rule, skbMark int, Op string) bool {
    var status bool           = false
    var matchOptions []string = IptablesMatchOptions(rule)
    // To set Skb Mark
    var cmdArgSkb    []string = []string{"-t", "mangle", Op, "POSTROUTING", "-j", "MARK", "--set-mark", strconv.Itoa(skbMark), "-m", "comment", "--comment", Conf.RuleComment}
    cmdArgSkb                 = append(cmdArgSkb, matchOptions...)
    status                    = IptablesExec(cmdArgSkb)
    if status == false {
        return status
    }

    // To set DSCP 
    var DSCPValue string      = "0x" + strconv.FormatInt(int64(rule.DSCP), 16)
    var cmdArgDSCP            = []string{"-t", "mangle", Op, "OUTPUT", "-j", "DSCP", "--set-dscp", DSCPValue, "-m", "comment", "--comment", "skbmark:"+strconv.Itoa(skbMark)}
    cmdArgDSCP                = append(cmdArgDSCP, matchOptions...)
    status                    = IptablesExec(cmdArgDSCP)
    if status == false {
        cmdArgSkb = []string{"-t", "mangle", "-D", "POSTROUTING", "-j", "MARK", "--set-mark", strconv.Itoa(skbMark), "-m", "comment", "--comment", Conf.RuleComment}
        cmdArgSkb = append(cmdArgSkb, matchOptions...)
        IptablesExec(cmdArgSkb)
        return status
    }
    return status
}
```



# 为什么RoCE网络需要QoS

RDMA最初设计用在运行高性能计算应用的infiniband网络中。Infiniband网络在协议规定上是无损网络，不会产生丢包现象。高性能计算应用通常会针对网络性能优化，因此拥有更友好的网络流量。进而，高性能计算网络对于QoS配置的需求也就更低。在另一方面，数据中心网络面对的是任意变化的流量场景，不同的网络流需要协调服务等级以提高网络效率。这产生更高的QoS要求来解决不同的问题。



# 硬件层如何对流量分类

在IP/Ethernet数据包头部中，有两种方式来对网络包进行分类：

- 使用VLAN头部的PCP域
- 使用IP头部的DSCP域

![image-20210715114831925](C:\Users\zzz48\AppData\Roaming\Typora\typora-user-images\image-20210715114831925.png)

![image-20210715114841721](C:\Users\zzz48\AppData\Roaming\Typora\typora-user-images\image-20210715114841721.png)

# 应用层如何对流量分类

为了满足不同应用需要不同等级的网络流服务，verbs接口和rdma_cm接口都向应用层提供了设置网络流优先级的QoS属性API。

类似于tcp/ip套接字中的setsockopt可以设置QoS参数。



# 应用层对流量的分类是怎么映射到硬件层上的分类

通常是要经过一个两阶段或者三阶段的过程来完成，但这在不同的库接口(verbs和rdma_cm)以及不同版本的RoCE协议下也是不同的。

首先，针对不同协议来看：

- RoCEv1：这个协议是将RDMA数据段封装到以太网数据段内，再加上以太网的头部，因此属于二层数据包，为了对它进行分类的话，只能使用VLAN头部中的PCP域来设置优先级值，因此还需要额外开启VLAN功能。当开启VLAN之后，在不同的库中映射过程如下：
  - verbs：
    - 应用在创建QP时，对QP属性中的SL(service level)字段进行设置优先级。
    - 在硬件驱动中，会自动将SL转换成VLAN头部中的PCP域代表的值UP，转换方式：UP = SL & 7，该值只有8种可选值
    - 网络流会根据UP值映射到对应的TC上
  - rdma_cm:
    - 应用通过rdma_set_option函数来设置ToS值，该值只有4种有效值：0,8,24,16，
    - 然后在内核中，将ToS转换成sk_prio，该值也只有4种有效值：0,2,4,6，映射方式固定
    - 最后在硬件驱动中，将sk_prio转换成UP，映射方式可以用户自定义
    - 网络流会根据UP值映射到对应的TC上
- RoCEv2: 这个协议是将RDMA数据段先封装到UDP数据段内，加上UDP头部，再加上IP头部，最后在加上以太网头部，属于三层数据包，为了对它进行分类的话，既可以使用以太网VLAN中的PCP域，也可以使用IP头部的DSCP域。对于PCP域的映射过程和上面一致，下面仅解释DSCP域的映射过程：
  - verbs:
    - 应用在创建QP时，对QP属性中GRH中的traffic_class字段进行设置优先级。
    - 在硬件驱动中，IP头部的ToS字段会直接被赋值为traffic_class，而DSCP只是ToS中的高6位，因此traffic_class到优先级的转换是：traffic_class=有效优先级值 * 4
    - 最终根据DSCP值到TC的映射表来将网络流映射到对应的TC上
  - rdma_cm:
    - 应用通过rdma_set_optin函数来设置ToS值
    - 在硬件驱动中，根据设置的ToS到DSCP值的映射表，将ToS转换成DSCP值
    - 最终根据DSCP值到TC的映射表来将网络流映射到对应的TC上



# 映射完成之后硬件是怎么针对优先级对网络流进行调度的

根据应用对网络流设置的优先级，最终将网络流映射到不同的TC上，而这些TC可以人为配置调度策略，网卡根据不同的调度策略来从不同的TC中向链路上发送数据。

一个流量类(TC)可以被赋予不同的服务质量属性，分别有：

- 严格优先级(Strict Priority)
- 最小带宽保证(Enhanced Transmission Selection, ETS)
- 速率限制(Rate Limit) (只具有该属性的tc又称为vendor)

## 严格优先级

具有严格优先级的TC比其他非严格优先级的流具有更高的优先级，在同是严格优先级的TC中，数字越大优先级越高。网卡总是先服务高优先级TC，仅当最高优先级的TC没有数据传输时才会去服务下一个最高优先级TC。使用严格优先级TC可以改善对于低延迟低带宽的网络流，但是不适合传输巨型数据，因为会使得系统中其他的传输者饥饿。

## 最小带宽保证

ETS利用提供给一个特定的流量类负载小于它的最小分配的带宽时剩余的时间周期，将这个可用的剩余时间差提供给其它流量类。

服务完严格优先级的TCs之后，链路上剩余的带宽会根据各自最小带宽保证比例分配给其它的TC。

## 速率限制

速率限制对一个TC定义了一个最大带宽值，这与ETS不同。



# 工具：mlnx_qos

### -i, --interface

This parameter is used to get the current QoS configuration:

```bash
# mlnx_qos -i eth2
```

### -f, --pfc

This parameter is used to set the Priority Flow Control (PFC) configuration on a port. You can enable any subset of priorities 0...7.

The priorities are provided using a comma-delimited list.

For example, to enable priority 3, run:

```bash
# mlnx_qos -i eth2 -f 0,0,0,1,0,0,0,0	
```

### -p --prio_tc

This parameter is used to map priorities to Egress Traffic Class (ETC).

**Note**: By default, priority 0 is mapped to tc1, and priority 1 is mapped to tc0. All other priorities are mapped to the same TC.

To change the mapping, add a command-delimited list. For example:

```bash
# mlnx_qos -i eth2 -p 0,1,2,3,4,5,6,7
```

### -s --tsa , -t -t-cbw

**-s** is used to set the Egress scheduling method, either **strict priority** or **ETS** per traffic class.

**-t** is used to set the bandwidth per traffic class (TC) (relevant when this TC egress scheduling method is ETS). The sum must be 100, while strict priority must get 0 bw.

These configurations must come together.

For example, to set strict priority to tc6, while all the rest are ETS, run:

```bash
# mlnx_qos -i eth2 -s ets,ets,ets,ets,ets,ets,strict,ets -t 10,10,10,10,10,10,0,40
```

### -r --ratelimit

This parameter is used to add a rate limit for each TC. For example, to limit tc0 to 5G, tc3 to 10G, and tc6 to 7G, run:

```bash
# mlnx_qos -i eth2 -r 5,0,0,10,0,0,7,0
```

### --trust

This parameter is used to set the trust level (pcp or dscp). 

```bash
# mlnx_qos -i eth2 --trust=dscp
```

### --dscp2prio

This parameter is used to set dscp to priority mapping when the trust state is "dscp". 

For example, to map dscp 30 to priority 6, run:

```bash
# mlnx_qos -i eth2 --dscp2prio set,30,6
```

## 附录：help

```bash
Options:
--version show program's version number and exit
-h, --help show this help message and exit
-f LIST, --pfc=LIST Set priority flow control for each priority. LIST is
comma separated value for each priority starting from
0 to 7. Example: 0,0,0,0,1,1,1,1 enable PFC on TC4-7
-p LIST, --prio_tc=LIST
maps UPs to TCs. LIST is 8 comma separated TC numbers.
Example: 0,0,0,0,1,1,1,1 maps UPs 0-3 to TC0, and UPs
4-7 to TC1
-s LIST, --tsa=LIST Transmission algorithm for each TC. LIST is comma
separated algorithm names for each TC. Possible
algorithms: strict, ets and vendor. Example:
vendor,strict,ets,ets,ets,ets,ets,ets sets TC0 to
vendor, TC1 to strict, TC2-7 to ets.
-t LIST, --tcbw=LIST Set minimal guaranteed %BW for ETS TCs. LIST is comma
separated percents for each TC. Values set to TCs that
are not configured to ETS algorithm must be zero.
Example: if TC0,TC2 are set to ETS, then
10,0,90,0,0,0,0,0 will set TC0 to 10% and TC2 to 90%.
Percents must sum to 100.
-r LIST, --ratelimit=LIST
Rate limit for TCs (in Gbps). LIST is a comma
separated Gbps limit for each TC. Example: 1,8,8 will
limit TC0 to 1Gbps, and TC1,TC2 to 8 Gbps each.
-d DCBX, --dcbx=DCBX set dcbx mode to firmware controlled(fw) or OS
controlled(os). Note, when in OS mode, mlnx_qos should
not be used in parallel with other dcbx tools such as
lldptool
--trust=TRUST set priority trust state to pcp or dscp
--dscp2prio=DSCP2PRIO
set/del a (dscp,prio) mapping. Example 'set,30,2' maps
dscp 30 to priority 2. 'del,30,2' deletes the mapping
and resets the dscp 30 mapping back to the default
setting priority 0.
--cable_len=CABLE_LEN
set cable_len for buffer's xoff and xon thresholds
--prio2buffer=LIST maps priority to receive buffer. Example:
0,2,5,7,1,2,3,6 maps priorities 0,1,2,3,4,5,6,7 to
receive buffer 0,2,5,7,1,2,3,6
--buffer_size=LIST Set receive buffer size LIST is comma separated
percents for each buffer.For pfc enabled buffer, the
buffer size must be larger than the xoff_threshold.
Example: 87296,87296,0,87296,0,0,0,0 sets receive
buffer size for buffer 0,1,2,3,4,5,6,7 respectively
-i INTF, --interface=INTF
Interface name
-a Show all interface's TCs
```

