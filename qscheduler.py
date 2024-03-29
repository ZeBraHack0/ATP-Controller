import Queue
import time
from SocketServer import ThreadingTCPServer, StreamRequestHandler
import socket
import threading
import sys


time_slice = 5
threshold = 60
starving = 3600
port = 8888
ban_list = []
cfq = {}
cft = {}
mutex_sch = threading.Lock()
mutex_cfq = threading.Lock()
link_capacity = 60  # Gbps
RTT = 70  # microsecond
PER_GPU = 2
DBL_MIN = float('-Infinity')
global fn
PTA = 10
WAITING = 0


port_of_worker = [56, 48, 40, 32, 24, 16, 8, 0, 4]
loop_back = [12, 28, 20, 44, 36, 60, 52]
MAC_address_of_worker = [ "b8:59:9f:1d:04:f2"
                        , "b8:59:9f:0b:30:72"
                        , "98:03:9b:03:46:50"
                        , "b8:59:9f:02:0d:14"
                        , "b8:59:9f:b0:2d:50"
                        , "b8:59:9f:b0:2b:b0"
                        , "b8:59:9f:b0:2b:b8"
                        , "b8:59:9f:b0:2d:18"
                        , "b8:59:9f:b0:2d:58" ]
ip_of_worker = [ "10.0.0.1"
                , "10.0.0.2"
                , "10.0.0.3"
                , "10.0.0.4"
                , "10.0.0.5"
                , "10.0.0.6"
                , "10.0.0.7"
                , "10.0.0.8"
                , "10.0.0.9" ]


def reduce(arr):
    sums = 0
    for x in arr:
        sums += x
    return sums


def bw_evaluation(server, tmp_job, job_trace, job_ps):
    m = job_trace.qsize()
    n = len(server)
    job_link = [[] for x in range(m)]
    server_link = [[] for x in range(n)]
    job_bw = [[0.0, 0.0] for x in range(m)]
    job_mark = [0 for x in range(m)]
    server_bw = [1.0 for x in range(n)]
    alloc = 0
    pta = PTA
    for i in range(m):
        job = job_trace.get()
        job_trace.put(job)
        ps = job_ps.get()
        job_ps.put(ps)
        if ps != -1:
            job_link[i].append(-1*ps-1)
            server_link[ps].append(-1*i-1)
            for p in job:
                job_link[i].append(p[0])
                server_link[p[0]].append(i)
        else:
            alloc += 1
    if len(tmp_job) > 1:
        m += 1
        job_link.append([])
        job_bw.append([0.0, 0.0])
        job_mark.append(0)
        for p in tmp_job:
            job_link[m-1].append(p[0])
            server_link[p[0]].append(m-1)

    while m > 0:
        # stage 1
        serverBN = [0.0 for x in range(n)]
        for i in range(n):
            min_bw = 1.1
            bottle = -1
            for j in range(n):
                if len(server_link[j]) == 0:
                    continue
                x = 0
                for k in server_link[j]:
                    if k >= 0:
                        x += 1
                    else:
                        kdx = -1 * (k + 1)
                        if pta > 0:
                            x += 1
                        else:
                            x += len(job_link[kdx]) - 1
                if server_bw[j] > 0 and x > 0 and server_bw[j] / x < min_bw:
                    bottle = j
                    min_bw = server_bw[j] / x
                serverBN[j] = server_bw[j] / x
            if pta > 1e-3 and m-alloc and pta / (m-alloc) < min_bw:
                min_bw = pta / (m-alloc)
            if bottle == -1:
                break
            bw = min_bw
            for j in range(m):
                jdx = j
                if job_mark[jdx] or len(job_link[jdx]) == 0:
                    continue

                for s in job_link[jdx]:
                    sdx = s
                    if sdx < 0:
                        sdx = -1 * (sdx + 1)
                    if serverBN[sdx] == bw:
                        alloc += 1
                        job_mark[jdx] = 1
                        break
                for s in job_link[jdx]:
                    sdx = s
                    if sdx < 0:
                        sdx = -1 * (sdx + 1)
                    if job_mark[jdx] == 1:
                        if s < 0:
                            server_link[sdx].remove(-1 * jdx - 1)
                        else:
                            server_link[sdx].remove(jdx)
                    if s < 0 and pta == 0:
                        server_bw[sdx] -= bw * (len(job_link[jdx]) - 1)
                    else:
                        server_bw[sdx] -= bw
                if pta == 0:  # add here
                    job_bw[jdx][1] += bw
                else:
                    job_bw[jdx][0] += bw
                    pta -= bw
            if alloc == m:
                break
        if alloc == m:
            break
    bandwidth = [x[0]+x[1] for x in job_bw]

    num = 0
    sums = 0.0
    for x in bandwidth:
        # if x > 0:
        #     sums += x
        #     num += 1
        if x > 1:
            print("warning!")
        if x > 0:
            sums += x
            num += 1
        else:
            sums += 0
            # num += 1
    if num > 0:
        avg = sums / num
    else:
        avg = 0
    # print(avg)
    return avg, bandwidth, server_bw


def packing(server, link, job_trace, job_ps, buffer, sums):
    n = len(server)
    jb = buffer.get()
    gpus = jb.cost
    if reduce(server) + gpus > sums:
        b = buffer.qsize()
        buffer.put(jb)
        for i in range(b):
            tmp3 = buffer.get()
            buffer.put(tmp3)
        return False, jb, [], -1
    # shadow link computing
    shadow_link = [0.0 for x in range(n)]
    empty = 0
    up_link = 0
    for i in range(n):
        shadow_link[i] = link[i]
        if server[i] < PER_GPU:
            up_link = max(up_link, link[i])
        if link[i] == 0:
            empty = 1

    avg, bandwidth, sbw = bw_evaluation(server, [], job_trace, job_ps)

    ls = []
    # connectionless solution
    if gpus <= PER_GPU:
        min_del = sys.maxsize
        min_link = 0
        min_bw = 0
        idx = -1
        for i in range(n):
            if PER_GPU - server[i] == gpus:
                idx = i
                break
            if PER_GPU - server[i] > gpus:
                if min_del > PER_GPU - server[i] - gpus:
                    min_del = PER_GPU - server[i] - gpus
                    min_link = link[i]
                    min_bw = sbw[i]
                    idx = i
                elif min_del == PER_GPU - server[i] - gpus:
                    if min_link < link[i]:
                        min_del = PER_GPU - server[i] - gpus
                        min_link = link[i]
                        min_bw = sbw[i]
                        idx = i
                    elif min_link == link[i]:
                        if min_bw < sbw[i]:
                            min_del = PER_GPU - server[i] - gpus
                            min_link = link[i]
                            min_bw = sbw[i]
                            idx = i
        if idx != -1:
            server[idx] += gpus
            job_trace.put([[idx, gpus]])
            job_ps.put(-1)
            return True, jb, [[idx, gpus]], -1

    # connection-oriented solution

    dp = [[DBL_MIN for x in range(gpus+PER_GPU+1)] for j in range(up_link+1)]
    trace = [[[[-1, -1] for x in range(gpus+PER_GPU+1)]for x in range(n)]for j in range(up_link+1)]
    for i in range(up_link+1):
        dp[i][0] = 1/(i+1)
    for i in range(n):
        w = PER_GPU - server[i]
        # v = -1 * (1 - 1 / (shadow_link[i] + 1))
        # v = 0
        # if shadow_link[i] > 0:
        #     v = -shadow_link[i]*(1/shadow_link[i]-1/(shadow_link[i]+1))
        v = sbw[i] - (1/(link[i]+1))
        l = link[i]
        for s in range(up_link + 1):
            for j in range(gpus+PER_GPU, -1, -1):
                trace[s][i][j] = trace[s][i-1][j]
        if w == 0:
            continue
        for s in range(up_link, -1, -1):
            wl = max(s, l)
            for j in range(gpus+PER_GPU, w-1, -1):
                if dp[s][j-w] != DBL_MIN and dp[wl][j] < dp[s][j-w] + v - 1/(s+1) + 1/(wl+1):
                    dp[wl][j] = dp[s][j-w] + v - 1/(s+1) + 1/(wl+1)
                    trace[wl][i][j] = [i, s]
    # decide solution
    ans = -1
    level = DBL_MIN
    state = 0
    for s in range(up_link + 1):
        if dp[s][gpus] > level:  # exist exact solution
            ans = gpus
            level = dp[s][gpus]
            state = s
    if ans == -1 or empty:
        for s in range(up_link + 1):
            for i in range(gpus+1, gpus+PER_GPU+1):
                if dp[s][i] == DBL_MIN:
                    continue
                tmp = dp[s][i]
                if tmp > level:
                    ans = i
                    level = tmp
                    state = s
    # print("gain: ", level)
    job = []
    cur_w = ans
    row = n-1
    while cur_w > 0:
        idx = trace[state][row][cur_w][0]
        pre_s = trace[state][row][cur_w][1]
        usage = PER_GPU - server[idx]
        job.append([idx, usage])
        cur_w -= usage
        state = pre_s
        row = idx - 1
    if ans > gpus:
        job = sorted(job, key=lambda x: x[1], reverse=True)
        for i in range(len(job)):
            if job[i][1] >= ans-gpus:
                job[i][1] -= ans-gpus
                break
    for j in job:
        server[j[0]] += j[1]
        link[j[0]] += 1

    # compute best ps
    pre = reduce(bandwidth)
    avg, bandwidth, sbw = bw_evaluation(server, job, job_trace, job_ps)
    for i in range(n):
        shadow_link[i] = link[i]
    m = job_trace.qsize()
    for i in range(m):
        ejob = job_trace.get()
        job_trace.put(ejob)
        ps = job_ps.get()
        job_ps.put(ps)
        if ps == -1:
            continue
        max_link = link[ps]
        for j in ejob:
            max_link = max(max_link, link[j[0]])
        for j in ejob:
            if link[j[0]] == 1 and max_link > 1:
                shadow_link[j[0]] = 0
        if link[ps] == 1 and max_link > 1:
            shadow_link[ps] = 0
    for i in range(n):
        ls.append([i, 1 - sbw[i], link[i]+shadow_link[i]])
    ls = sorted(ls, key=lambda x: [x[1], x[2]])
    job_ps.put(ls[0][0])
    # print(ls[0][1])
    link[ls[0][0]] += 1
    job_trace.put(job)
    avg, bandwidth, sbw = bw_evaluation(server, [], job_trace, job_ps)
    if reduce(bandwidth) - pre < WAITING:
        for j in job:
            server[j[0]] -= j[1]
            link[j[0]] -= 1
        link[ls[0][0]] -= 1
        m = job_trace.qsize()
        for i in range(m):
            tmp1 = job_trace.get()
            tmp2 = job_ps.get()
            if i != m-1:
                job_trace.put(tmp1)
                job_ps.put(tmp2)
        b = buffer.qsize()
        buffer.put(jb)
        for i in range(b):
            tmp3 = buffer.get()
            buffer.put(tmp3)
        return False, jb, job, ls[0][0]
    else:
        return True, jb, job, ls[0][0]


def topo(file="controller/topology", initial=False):
    f = open(file)
    s = f.readlines()
    host_num = int(s[0].strip('\n'))
    # port_of_worker = []
    # ip_of_worker = []
    # MAC_address_of_worker = []
    # for i in range(host_num):
    #     port_of_worker.append([int(x.strip('\n')) for x in s[i+1].split('\t')])
    # for i in range(host_num):
    #     ip_of_worker.append([x.strip('\n') for x in s[i+1+host_num].split('\t')])
    # for i in range(host_num):
    #     MAC_address_of_worker.append([x.strip('\n') for x in s[i+1+2*host_num].split('\t')])
    # single_loopback_port = loop_back

    # if not initial:
    #     return port_of_worker, ip_of_worker, MAC_address_of_worker, single_loopback_port

    # initial switch
    clear_all()

    p4_pd.register_reset_all_agtr_time()
    p4_pd.register_reset_all_appID_and_Seq()
    p4_pd.register_reset_all_bitmap()
    p4_pd.register_reset_all_register1()
    p4_pd.register_reset_all_register2()
    p4_pd.register_reset_all_register3()
    p4_pd.register_reset_all_register4()
    p4_pd.register_reset_all_register5()
    p4_pd.register_reset_all_register6()
    p4_pd.register_reset_all_register7()
    p4_pd.register_reset_all_register8()
    p4_pd.register_reset_all_register9()
    p4_pd.register_reset_all_register10()
    p4_pd.register_reset_all_register11()
    p4_pd.register_reset_all_register12()
    p4_pd.register_reset_all_register13()
    p4_pd.register_reset_all_register14()
    p4_pd.register_reset_all_register15()
    p4_pd.register_reset_all_register16()
    p4_pd.register_reset_all_register17()
    p4_pd.register_reset_all_register18()
    p4_pd.register_reset_all_register19()
    p4_pd.register_reset_all_register20()
    p4_pd.register_reset_all_register21()
    p4_pd.register_reset_all_register22()
    p4_pd.register_reset_all_register23()
    p4_pd.register_reset_all_register24()
    p4_pd.register_reset_all_register25()
    p4_pd.register_reset_all_register26()
    p4_pd.register_reset_all_register27()
    p4_pd.register_reset_all_register28()
    p4_pd.register_reset_all_register29()
    p4_pd.register_reset_all_register30()
    p4_pd.register_reset_all_register31()

    # basic routine
    len_workers = len(port_of_worker)
    for i in range(0, len_workers):
        p4_pd.forward_table_add_with_set_egr(
            p4_pd.forward_match_spec_t(macAddr_to_string(MAC_address_of_worker[i])),
            p4_pd.set_egr_action_spec_t(port_of_worker[i])
        )
    print("basic routine done")

    # config drop table
    for w in port_of_worker:
        p4_pd.drop_table_table_add_with_drop_pkt(
            p4_pd.drop_table_match_spec_t(
            w,
            0)
        )

    print("drop_table done")

    p4_pd.modify_packet_bitmap_table_table_add_with_modify_packet_bitmap(
        p4_pd.modify_packet_bitmap_table_match_spec_t(1)
    )

    p4_pd.modify_packet_bitmap_table_table_add_with_nop(
        p4_pd.modify_packet_bitmap_table_match_spec_t(0)
    )

    p4_pd.processEntry1_table_add_with_processentry1(
        p4_pd.processEntry1_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry1_table_add_with_noequ0_processentry1(
        p4_pd.processEntry1_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1,
    )
    p4_pd.processEntry2_table_add_with_processentry2(
        p4_pd.processEntry2_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry2_table_add_with_noequ0_processentry2(
        p4_pd.processEntry2_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry3_table_add_with_processentry3(
        p4_pd.processEntry3_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry3_table_add_with_noequ0_processentry3(
        p4_pd.processEntry3_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry4_table_add_with_processentry4(
        p4_pd.processEntry4_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry4_table_add_with_noequ0_processentry4(
        p4_pd.processEntry4_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry5_table_add_with_processentry5(
        p4_pd.processEntry5_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry5_table_add_with_noequ0_processentry5(
        p4_pd.processEntry5_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry6_table_add_with_processentry6(
        p4_pd.processEntry6_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry6_table_add_with_noequ0_processentry6(
        p4_pd.processEntry6_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry7_table_add_with_processentry7(
        p4_pd.processEntry7_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry7_table_add_with_noequ0_processentry7(
        p4_pd.processEntry7_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry8_table_add_with_processentry8(
        p4_pd.processEntry8_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry8_table_add_with_noequ0_processentry8(
        p4_pd.processEntry8_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry9_table_add_with_processentry9(
        p4_pd.processEntry9_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry9_table_add_with_noequ0_processentry9(
        p4_pd.processEntry9_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry10_table_add_with_processentry10(
        p4_pd.processEntry10_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry10_table_add_with_noequ0_processentry10(
        p4_pd.processEntry10_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry11_table_add_with_processentry11(
        p4_pd.processEntry11_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry11_table_add_with_noequ0_processentry11(
        p4_pd.processEntry11_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry12_table_add_with_processentry12(
        p4_pd.processEntry12_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry12_table_add_with_noequ0_processentry12(
        p4_pd.processEntry12_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry13_table_add_with_processentry13(
        p4_pd.processEntry13_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry13_table_add_with_noequ0_processentry13(
        p4_pd.processEntry13_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry14_table_add_with_processentry14(
        p4_pd.processEntry14_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry14_table_add_with_noequ0_processentry14(
        p4_pd.processEntry14_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry15_table_add_with_processentry15(
        p4_pd.processEntry15_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry15_table_add_with_noequ0_processentry15(
        p4_pd.processEntry15_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry16_table_add_with_processentry16(
        p4_pd.processEntry16_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry16_table_add_with_noequ0_processentry16(
        p4_pd.processEntry16_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry17_table_add_with_processentry17(
        p4_pd.processEntry17_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry17_table_add_with_noequ0_processentry17(
        p4_pd.processEntry17_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry18_table_add_with_processentry18(
        p4_pd.processEntry18_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry18_table_add_with_noequ0_processentry18(
        p4_pd.processEntry18_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry19_table_add_with_processentry19(
        p4_pd.processEntry19_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry19_table_add_with_noequ0_processentry19(
        p4_pd.processEntry19_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry20_table_add_with_processentry20(
        p4_pd.processEntry20_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry20_table_add_with_noequ0_processentry20(
        p4_pd.processEntry20_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry21_table_add_with_processentry21(
        p4_pd.processEntry21_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry21_table_add_with_noequ0_processentry21(
        p4_pd.processEntry21_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry22_table_add_with_processentry22(
        p4_pd.processEntry22_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry22_table_add_with_noequ0_processentry22(
        p4_pd.processEntry22_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry23_table_add_with_processentry23(
        p4_pd.processEntry23_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry23_table_add_with_noequ0_processentry23(
        p4_pd.processEntry23_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry24_table_add_with_processentry24(
        p4_pd.processEntry24_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry24_table_add_with_noequ0_processentry24(
        p4_pd.processEntry24_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry25_table_add_with_processentry25(
        p4_pd.processEntry25_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry25_table_add_with_noequ0_processentry25(
        p4_pd.processEntry25_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry26_table_add_with_processentry26(
        p4_pd.processEntry26_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry26_table_add_with_noequ0_processentry26(
        p4_pd.processEntry26_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry27_table_add_with_processentry27(
        p4_pd.processEntry27_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry27_table_add_with_noequ0_processentry27(
        p4_pd.processEntry27_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry28_table_add_with_processentry28(
        p4_pd.processEntry28_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry28_table_add_with_noequ0_processentry28(
        p4_pd.processEntry28_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry29_table_add_with_processentry29(
        p4_pd.processEntry29_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry29_table_add_with_noequ0_processentry29(
        p4_pd.processEntry29_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry30_table_add_with_processentry30(
        p4_pd.processEntry30_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry30_table_add_with_noequ0_processentry30(
        p4_pd.processEntry30_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    p4_pd.processEntry31_table_add_with_processentry31(
        p4_pd.processEntry31_match_spec_t(hex_to_i32(0), hex_to_i32(0xFFFFFFFF)), 1,
    )
    p4_pd.processEntry31_table_add_with_noequ0_processentry31(
        p4_pd.processEntry31_match_spec_t(hex_to_i32(0), hex_to_i32(0x00000000)), 1
    )
    print("finish topo building")


def config(job_id, workers, ps, single_loopback_port):
    # Here we assume that one job has only one Ps
    # config outPort table
    for w in workers:
        p4_pd.outPort_table_table_add_with_set_egr_and_set_index(
            p4_pd.outPort_table_match_spec_t(
                job_id << 16,
                w,  # port of worker
                0,  # if the packet is recirculate (0: no / 1: yes)
                0),  # ps index, always 1
            p4_pd.set_egr_and_set_index_action_spec_t(single_loopback_port))

    print("outPort table 1 done")

    p4_pd.outPort_table_table_add_with_set_egr(
        p4_pd.outPort_table_match_spec_t(
            job_id << 16,
            single_loopback_port,
            1,
            0),
        p4_pd.set_egr_action_spec_t(ps))

    print("outPort table 2 done")

    # config multicast for Ps

    p4_pd.multicast_table_table_add_with_multicast(
        p4_pd.multicast_table_match_spec_t(
            1,
            job_id << 16,
            ps,
            0),
        # multicast app1 -> worker1, 2
        p4_pd.multicast_action_spec_t(1000-job_id)
    )
    print("finish configuration")


def deconfig(job_id, workers, ps, single_loopback_port):

    # Here we assume that one job has only one Ps
    # config outPort table
    for w in workers:
        p4_pd.outPort_table_table_delete_by_match_spec(
            p4_pd.outPort_table_match_spec_t(
                job_id << 16,
                w,
                0,
                0))

    print("outPort table 1 deleted")


    p4_pd.outPort_table_table_delete_by_match_spec(
        p4_pd.outPort_table_match_spec_t(
            job_id << 16,
            single_loopback_port,
            1,
            0))

    print("outPort table 2 deleted")


    # config multicast for Ps

    p4_pd.multicast_table_table_delete_by_match_spec(
        p4_pd.multicast_table_match_spec_t(
            1,
            job_id << 16,
            ps,
            0)
    )
    print("finish deletion")


class Job:
    def __init__(self, s1="", s2="", s3="", cost=1, dataset="", model="", num_iters=10):
        self.cost = cost  # gpu num
        self.dis = []  # distribution of workers
        self.gpus = []  # bitmap of GPUs
        self.id = 0
        self.dataset = dataset
        self.model = model
        self.num_iters = num_iters
        self.waiting_time = 0
        self.ps = -1
        self.loopback = -1
        if s1 != "":
            self.id = int(s1.strip('\n'))
            self.waiting_time = int(s2.strip('\n'))
            self.dis.append([(b for b in x.strip('\n').split(',')) for x in s3.split('\t')])  # last pair represents the ps
            self.cost = len(self.dis)

    def set(self, id, wt, ds):
        self.id = id
        self.waiting_time = wt
        self.dis = ds
        self.cost = len(self.dis)


def send_executor(role, des, idx, workerID, workerSum, tar_job, pid):
    appID = tar_job.id
    gpu_num = 0
    if role == "worker":
        message = "execute_worker_" + str(workerID) + "_" + str(workerSum) + "_" + str(appID) + "_"
        for m in tar_job.gpus:
            if m[0] == pid:
                message += str(m[1])
                message += ","
                gpu_num += 1
        message = message.strip(",")
        message += "_" + tar_job.dataset + "_" + tar_job.model + "_" + str(tar_job.num_iters)
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print((des, port))
        # handshaking
        flag = 0
        while flag == 0:
            time.sleep(1)
            try:
                if flag == 0:
                    client.connect((des, port))
                    print("worker " + str(workerID) + " connected")
                    flag = 1
            except Exception as e:
                print(e)

        print(appID, "send worker request")
        client.send(message.encode('utf-8'))
        data = client.recv(1024)
        if data.decode('utf-8') == "finished!":
            if mutex_cfq.acquire():
                cfq[idx] += gpu_num
                mutex_cfq.release()
            return

    elif role == "ps":
        message = "execute_ps_"+str(tar_job.id)
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # handshaking
        flag = 0
        while flag == 0:
            time.sleep(1)
            try:
                if flag == 0:
                    client.connect((des, port))
                    print("server connected")
                    flag = 1
            except Exception as e:
                print(e)

        print(appID, "send ps request")
        client.send(message.encode('utf-8'))
        client.close()
        while True:
            time.sleep(5)
            if cfq[idx] == tar_job.cost-1:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect((des, 10000+appID))
                message = "execute_finished"
                client.send(message.encode('utf-8'))
                client.close()
                if mutex_cfq.acquire():
                    cfq[idx] += 1
                    mutex_cfq.release()
                print(idx, "execute_finished")
                print(cfq)
                # client.send(message.encode('utf-8'))
                # data = client.recv(1024)
                # if data.decode('utf-8') == "finished!":
                return


class Scheduler:
    def __init__(self):
        self.job_num = 0
        self.job_id = [0 for i in range(0, 999)]
        self.rc = 0
        self.rc_sum = 0
        self.q1 = []
        self.dis = []  # GPU bitmap
        self.gpus = []  # PER_GPU
        self.mc_node = {}
        self.worker_num = 0
        self.idx = 0
        self.port_of_worker, self.ip_of_worker, self.MAC_address_of_worker, self.single_loopback_port = port_of_worker, ip_of_worker, MAC_address_of_worker, loop_back
        self.loop_use = [0 for x in self.single_loopback_port]
        self.loop_num = len(self.single_loopback_port)
        # for placement
        self.server = []
        self.link = []
        self.job_trace = Queue.Queue()
        self.job_ps = Queue.Queue()
        self.buffer = Queue.Queue()
        self.ptov = {}
        self.vtop = {}
        for w in self.port_of_worker:
            self.dis.append([])  # usage bitmap of GPUs
            self.gpus.append(0)  # total
            # self.rc += len(w)
        topo()
        # handshaking
        for i in range(len(self.port_of_worker)):
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                client.connect((self.ip_of_worker[i], port))
                client.send("gpu".encode('utf-8'))
                data = client.recv(1024).decode('utf-8')
                self.rc += int(data)
                self.dis[i].extend([0 for x in range(int(data))])
                self.gpus[i] = int(data)
                client.close()
                print("worker " + str(i) + " with " + data + "GPUs connected!")
            except Exception as e:
                # print(e)
                # ban_list.append(i)
                print("worker " + str(i)+" disconnected!")
        self.rc_sum = self.rc
        print("total GPU: " + str(self.rc_sum))
        for i in range(len(self.gpus)):
            if self.gpus[i] > 0:
                self.server.append(0)
                self.link.append(0)
                self.ptov[i] = len(self.server)-1
                self.vtop[len(self.server)-1] = i

        print("scheduler init success!")

    # def read_status(self, file_status="controller/status"):
    #     # read status info
    #     f1 = open(file_status)
    #     s = f1.readlines()
    #     self.job_num = int(s[0].strip('\n'))
    #     self.worker_num = int(s[1].strip('\n'))
    #     if self.job_num == 0:
    #         return
    #     i = 0
    #     while i < len(s):
    #         self.jobs.append(Job(s[i], s[i + 1], s[i+2]))
    #         self.job_id[self.jobs[-1].id] = 1
    #         i += 3
    #     self.workload = [0 for i in range(0, self.worker_num)]
    #     self.psload = [0 for i in range(0, self.worker_num)]
    #     for j in self.jobs:
    #         for w in j.dis:
    #             self.dis[w[0]][w[1]] = 1
    #             self.workload[w[0]] += 1
    #         self.psload[j.dis[-1][0]] += 1
    #         self.rc -= j.cost

    def place(self):
        # try placement
        f, job, job.dis, job.ps = packing(self.server, self.link, self.job_trace, self.job_ps, self.buffer, self.rc_sum)
        if not f:
            return False
        print("place job " + str(job.id))
        print(job.dis)
        print(job.ps)

        # decide new job IDs
        avail = -1  # 1000 - appID
        for i in range(1, 999):
            if self.job_id[i] == 0:  # 1 for using , 0 for not register, -1 for having released
                avail = i
                break
        if avail == -1:
            print("multicast number error")
            quit()
        tmp_id = job.id
        job.id = avail


        for m in job.dis:
            # decide GPU distribution
            idx = self.vtop[m[0]]
            cnt = 0
            for i in range(len(self.dis[idx])):
                if self.dis[idx][i] == 0:
                    self.dis[idx][i] = 1
                    job.gpus.append([idx, i])
                    print([idx, i])
                    cnt += 1
                if cnt >= m[1]:
                    break

        self.rc -= job.cost
        self.job_num += 1

        pdis = []
        for m in job.dis:
            pdis.append([self.vtop[m[0]], m[1]])

        # install rules
        worker = [self.port_of_worker[x[0]] for x in pdis]
        if len(job.dis) > 1:  # need a ps and install rules
            # decide single_loop_back
            i = 0
            single_loop_back = -1
            for bit in self.loop_use:
                if bit < 1:
                    single_loop_back = i
                    self.loop_use[i] += 1
                    self.loop_num -= 1
                    break
                i += 1
            if single_loop_back == -1:
                print("loopback selection error")
                return -1
            else:
                print("single_loop_back: " + str(single_loop_back))
            job.loopback = single_loop_back
            config(job.id, worker, self.port_of_worker[self.vtop[job.ps]], self.single_loopback_port[single_loop_back])
            print([job.id, worker, self.port_of_worker[self.vtop[job.ps]], self.single_loopback_port[single_loop_back]])
            print("occupy mc_node: "+str(avail))
            if self.job_id[avail] == 0:
                mcg_all = mc.mgrp_create(1000 - avail)
                node_all = mc.node_create(
                    rid=1000 - avail,
                    port_map=devports_to_mcbitmap(worker),
                    lag_map=lags_to_mcbitmap(([]))
                )
                mc.associate_node(mcg_all, node_all, xid=0, xid_valid=False)
                conn_mgr.complete_operations()
                self.mc_node[avail] = [node_all, mcg_all]
            else:
                print("mc_node reuse!")
                mc.node_update(
                    self.mc_node[avail][0],
                    port_map=devports_to_mcbitmap(worker),
                    lag_map=lags_to_mcbitmap(([]))
                )


        self.job_id[avail] = 1

        # send message
        worker_sum = len(worker)
        threads = []
        if len(job.dis) > 1:  # need a ps
            pt = threading.Thread(target=send_executor, args=("ps", self.ip_of_worker[self.vtop[job.ps]], tmp_id, 0, worker_sum, job, self.vtop[job.ps]))
            pt.setDaemon(True)
            threads.append(pt)
        for i in range(worker_sum):
            th = threading.Thread(target=send_executor, args=("worker", self.ip_of_worker[pdis[i][0]], tmp_id, i, worker_sum, job, pdis[i][0]))
            th.setDaemon(True)
            threads.append(th)
        for th in threads:
            th.start()

        # generate config_file
        conf = open("controller/config_"+str(job.id)+"_"+str(time.time()), "w")
        conf.write(str(job.id) + '\n')
        worker_place = ""
        for x in pdis:
            if worker_place != "":
                worker_place += '\t'
            worker_place += str(self.port_of_worker[x[0]])
        worker_place += '\n'
        conf.write(worker_place)
        if len(job.dis) > 1:  # need a ps
            conf.write(str(self.port_of_worker[self.vtop[job.ps]]) + '\n')
        conf.close()

        cft[tmp_id] = time.time()
        return True

    def schedule(self):
        if self.job_num > 999:
            print("overload")
            quit()

        i = 0
        while i < len(self.q1):
            if self.rc <= 0:
                break
            if self.q1[i].cost <= self.rc and self.loop_num > 0:
                self.buffer.put(self.q1[i])
                self.q1.pop(i)
                i -= 1
            else:
                break
            i += 1
        while self.buffer.qsize():
            f = self.place()
            if not f:
                break
            else:
                print("rest resources:" + str(self.rc))

    def receive_job(self, j):
        self.idx += 1
        cfq[self.idx] = -1
        j.id = self.idx  # store the idx temporarily
        self.q1.append(j)
        self.schedule()
        return self.idx



sch = Scheduler()


class myTCP(StreamRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        print("receive from (%r):%r" % (self.client_address, data.decode('utf-8')))
        s = data.decode('utf-8').split("_")
        idx = -1
        if s[0] == "control":
            if mutex_sch.acquire():
                job = Job(cost=int(s[1]), model=s[3], dataset=s[2], num_iters=int(s[4]))
                if job.cost > sch.rc_sum:
                    self.wfile.write("resource overload!".encode('utf-8'))
                    return
                idx = sch.receive_job(job)
                mutex_sch.release()
                begin_time = 0
                while True:
                    time.sleep(1)
                    if mutex_sch.acquire():
                        if (len(job.dis) > 1 and cfq.get(idx, -1) >= job.cost) or (len(job.dis) == 1 and cfq.get(idx, -1) >= job.cost-1):  # check cfq
                            end_time = time.time()
                            f = open("controller/DE.txt", "a")
                            f.write(str(end_time-cft[idx])+"\n")
                            f.close()
                            print("finished job "+str(idx)+"!")
                            print(job.dis)
                            self.wfile.write("finished!".encode('utf-8'))
                            # release resources
                            for w in job.gpus:
                                sch.dis[w[0]][w[1]] = 0
                            for p in job.dis:
                                sch.server[p[0]] -= p[1]
                                if len(job.dis) > 1:
                                    sch.link[p[0]] -= 1
                            if job.ps != -1:
                                sch.link[job.ps] -= 1
                                sch.loop_use[job.loopback] -= 1
                                sch.loop_num += 1
                            sch.rc += job.cost
                            if job.id in sch.mc_node:
                                sch.job_id[job.id] = -1
                            else:
                                sch.job_id[job.id] = 0
                            sch.job_num -= 1
                            print(sch.server)
                            print(sch.link)
                            print(sch.dis)
                            # delete rules
                            if len(job.dis) > 1:  # need a ps
                                worker = [sch.port_of_worker[sch.vtop[x[0]]] for x in job.dis]
                                deconfig(job.id, worker, sch.port_of_worker[sch.vtop[job.ps]], sch.single_loopback_port[job.loopback])
                            m = sch.job_ps.qsize()
                            for i in range(m):
                                jtmp = sch.job_trace.get()
                                ptmp = sch.job_ps.get()
                                if jtmp != job.dis:
                                    sch.job_ps.put(ptmp)
                                    sch.job_trace.put(jtmp)
                            mutex_sch.release()
                            break
                        mutex_sch.release()
        else:
            print("error message!")
            return


def run_schedulor():
    while True:
        time.sleep(1)
        if mutex_sch.acquire():
            sch.schedule()
            # print("rest resources:"+str(sch.rc))
            avg, bandwidth, sbw = bw_evaluation(sch.server, [], sch.job_trace, sch.job_ps)
            # if len(sbw) > 2:
            #     f = open("controller/BW.txt", "a")
            #     f.write(str(1-sbw[2]) + "\n")
            #     f.close()
            mutex_sch.release()

            
def hex_to_i32(h):
    x = int(h, 0)
    if (x > 0xFFFFFFFF):
        raise UIn_Error("Integer cannot fit within 32 bits")
    if (x > 0x7FFFFFFF): x-= 0x100000000
    return x


if __name__ == '__main__':
    ip = "10.0.0.10"
    print(ip)
    addr = (ip, port)
    print("server up")
    t = threading.Thread(target=run_schedulor)
    t.setDaemon(True)
    t.start()
    server = ThreadingTCPServer(addr, myTCP)
    server.serve_forever()
