import queue
import numpy as np
from scipy import optimize as op
import matplotlib.pyplot as plt
import sys
import random
import math

MACHINE = 5
PER_GPU = 2
DBL_MIN = float('-Infinity')
JOB_TIME = 5
COM_TIME = 0
READ_INF = 0
CCR = 1
PTA = 10
WAITING = 0

# real
# trace = [8, 8, 4, 16, 8, 16, 8, 4, 4, 4, 4, 16, 4, 4, 8, 8, 4, 4, 2, 2, 4, 8, 8, 4, 16, 8, 16, 32, 4, 8, 4, 2, 4, 8, 4, 4, 8, 8, 8, 8, 8, 4, 8, 8, 8, 8, 2, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 4, 8, 8, 8, 4, 4, 24, 4, 16, 2, 8, 16, 4, 8, 4, 4, 16, 8, 8, 8, 4, 8, 8, 4, 4, 8, 8, 4, 8, 4, 4, 4, 4, 8, 4, 8]
# trace = [5, 4, 2, 5, 4, 5, 5, 2, 3, 3, 2, 6, 3, 3, 3, 4, 3, 2, 1, 1, 3, 5, 5, 2, 6, 3, 7, 6, 2, 3, 2, 1, 3, 4, 3, 2, 5, 5, 4, 5, 4, 2, 4, 5, 3, 3, 1, 3, 4, 3, 3, 4, 3, 5, 3, 5, 3, 3, 4, 3, 5, 5, 3, 5, 2, 5, 4, 3, 3, 2, 5, 2, 5, 1, 3, 5, 3, 5, 3, 3, 7, 5, 3, 5, 2, 5, 3, 2, 2, 3, 5, 2, 4, 3, 3, 3, 2, 3, 2, 3]
# trace = [3, 3, 2, 4, 3, 6, 3, 2, 2, 3, 2, 4, 2, 2, 3, 4, 2, 2, 1, 1, 3, 3, 5, 3, 6, 5, 5, 7, 3, 3, 3, 1, 3, 5, 3, 2, 5, 5, 5, 5, 5, 3, 5, 3, 5, 4, 1, 5, 3, 5, 4, 3, 3, 3, 3, 3, 4, 5, 4, 5, 5, 4, 4, 3, 3, 4, 3, 3, 2, 3, 4, 2, 6, 1, 5, 6, 3, 5, 3, 3, 4, 4, 4, 5, 3, 5, 5, 3, 3, 3, 3, 3, 3, 2, 2, 3, 3, 4, 3, 5]
# for i in range(len(trace)):
#     base = int(math.log(trace[i], 2))
#     trace[i] = random.randint(base, base*2-1)
# trace = trace[:20]

# norm
# trace = [3, 7, 5, 3, 5, 4, 6, 1, 5, 4, 4, 8, 2, 7, 4, 3, 9, 2, 2, 7, 6, 3, 5, 2, 6, 6, 4, 7, 3, 5, 4, 7, 8, 8, 5, 5, 5, 5, 4, 6, 8, 4, 5, 9, 3, 8, 6, 5, 4, 5]
# trace = [5, 5, 4, 4, 3, 4, 2, 8, 3, 4, 1, 4, 7, 6, 4, 4, 1, 3, 2, 8, 5, 4, 0, 1, 1, 6, 5, 1, 4, 8, 5, 6, 6, 4, 1, 2, 4, 4, 2, 3, 3, 0, 3, 0, 6, 5, 3, 7, 0, 5, 7, 5, 8, 2, 6, 6, 0, 5, 6, 3, 6, 3, 4, 2, 4, 1, 3, 9, 1, 4, 7, 4, 5, 3, 4, 3, 5, 5, 1, 3, 5, 5, 2, 2, 4, 7, 1, 6, 2, 0, 3, 2, 1, 3, 4, 3, 6, 2, 2, 2]
# trace = np.random.normal(PER_GPU, 2, 20).astype(int)

# poisson
# trace = [6, 4, 6, 2, 4, 5, 9, 2, 4, 4, 3, 7, 4, 5, 2, 5, 4, 1, 3, 0, 4, 7, 3, 2, 1, 3, 4, 4, 3, 2, 3, 2, 4, 4, 0, 5, 7, 3, 0, 6, 5, 1, 4, 1, 4, 5, 4, 5, 3, 2, 2, 8, 4, 3, 3, 4, 5, 4, 4, 4, 6, 7, 7, 5, 4, 9, 2, 4, 1, 4, 2, 4, 4, 4, 3, 4, 6, 2, 4, 9, 1, 3, 2, 3, 0, 2, 6, 1, 0, 3, 4, 5, 6, 6, 0, 6, 4, 4, 6, 8]
# trace = [7, 3, 4, 1, 3, 4, 6, 2, 6, 2, 6, 1, 5, 5, 1, 3, 2, 2, 2, 5, 4, 5, 3, 3, 6, 7, 1, 4, 2, 10, 6, 3, 6, 5, 1, 5, 8, 1, 2, 3, 4, 4, 4, 8, 6, 4, 9, 1, 5, 1, 3, 4, 4, 0, 2, 3, 3, 1, 1, 4, 3, 5, 7, 4, 6, 6, 5, 6, 6, 5, 1, 5, 3, 4, 0, 1, 4, 2, 6, 1, 8, 5, 8, 5, 3, 5, 4, 5, 2, 2, 6, 3, 4, 6, 3, 3, 2, 3, 8, 2]
# trace = np.random.poisson(PER_GPU, 20)
trace = [3, 1, 3, 2, 4, 3, 1, 2, 2, 2, 1, 1, 3, 3, 1, 2, 1, 2, 1, 6]

period = np.array([16.966666666666665, 3221.05, 3318.3333333333335, 3112.4333333333334, 14.266666666666667, 5726.25, 1698.5333333333333, 1694.3166666666666, 1689.5333333333333, 15144.35, 3759.2333333333336, 172.46666666666667, 5672.7, 1639.5333333333333, 2996.1833333333334, 1622.1166666666668, 1622.3, 669.7666666666667, 10024.716666666667, 1592.1333333333332, 5096.55, 5594.383333333333, 1534.8666666666666, 1527.9666666666667, 12722.566666666666, 26094.916666666668, 1513.1666666666667, 11185.216666666667, 3394.35, 1488.2333333333333, 161.06666666666666, 401.43333333333334, 15780.566666666666, 2956.2, 3115.733333333333, 1936.7666666666667, 1407.9666666666667, 5431.583333333333, 5416.116666666667, 5414.383333333333, 1387.6833333333334, 3145.4166666666665, 4894.15, 3398.9833333333336, 1330.3166666666666, 3113.733333333333, 1321.0666666666666, 3406.7666666666664, 1295.6833333333334, 5308.433333333333, 2929.1666666666665, 1264.6833333333334, 3197.0333333333333, 5643.116666666667, 9866.2, 3372.85, 2757.05, 2278.0166666666664, 4698.016666666666, 1151.3833333333334, 2798.8, 1142.65, 1653.2833333333333, 1094.0833333333333, 9703.716666666667, 3388.0833333333335, 45.03333333333333, 2433.5166666666664, 2054.0166666666664, 65.43333333333334, 962.5666666666667, 4466.266666666666, 45816.95, 32654.15, 33076.416666666664, 32653.933333333334, 32656.333333333332, 15705.3, 791.0833333333334, 789.0666666666667, 783.6833333333333, 782.4333333333333, 617.0166666666667, 14216.483333333334, 683.3833333333333, 2764.2333333333336, 2752.866666666667, 3183.2, 661.9333333333333, 1365.8833333333334, 3166.6666666666665, 655.0166666666667, 13519.233333333334, 2751.416666666667, 6299.95, 8833.616666666667, 6212.833333333333, 21.616666666666667, 3019.0333333333333, 6163.166666666667, 1624.0166666666667, 15.483333333333333, 15.366666666666667, 272.98333333333335, 254.36666666666667, 2885.266666666667, 1111.35, 1107.85, 13.85, 15.366666666666667, 49.333333333333336, 17.183333333333334, 16.05, 4369.716666666666, 17.6, 15.25, 1415.25, 736.2666666666667, 14.216666666666667, 3395.55, 223.45, 255.16666666666666, 255.28333333333333, 256.35, 249.93333333333334, 255.26666666666668, 160.68333333333334, 160.58333333333334, 205.7, 38.55, 118.55, 217.66666666666666, 22.333333333333332, 17853.35, 17.266666666666666, 2758.0666666666666, 987.4333333333333, 46.63333333333333, 979.1833333333333, 24.25, 12.05, 11.75, 10359.033333333333, 14.716666666666667, 4141.883333333333, 810.7166666666667, 5739.55, 3696.9166666666665, 2560.25, 20.233333333333334, 1400.0833333333333, 2819.0, 2497.15, 2478.15, 1469.85, 15427.183333333334, 99.95, 3056.866666666667, 2370.6666666666665, 3034.0333333333333, 327.43333333333334, 2336.35, 2841.083333333333, 1856.05, 2267.0666666666666, 835.4666666666667, 789.2666666666667, 2201.0333333333333, 775.4166666666666, 90.5, 709.3833333333333, 1322.6166666666666, 3361.383333333333, 1203.7333333333333, 48.916666666666664, 147.56666666666666, 1226.8666666666666, 50.0, 1200.7666666666667, 23602.216666666667, 2015.2333333333333, 207.33333333333334, 3506.616666666667, 1353.0333333333333, 1203.6833333333334, 1212.6333333333334, 625.85, 11.833333333333334, 16.6, 49.88333333333333, 2763.7666666666664, 3135.6, 2752.383333333333, 1744.0333333333333, 2304.366666666667, 893.5666666666667, 9329.983333333334, 1658.1, 2453.0, 1645.5666666666666])
period = np.log(period).astype(int)
if type(trace) == list:
    print(trace)
else:
    print(trace.tolist())


def update(server, link, job_trace, job_load, bandwidth, job_ps, ce):
    release = 0
    workload = 0
    for i in range(len(bandwidth)):
        job = job_trace.get()
        load = job_load.get()
        ps = job_ps.get()
        load[2] += 1
        if load[0] > 0 and load[1] <= 0:
            load[0] -= 1
            if load[0] >= 0:
                workload += 1
            load[1] = CCR
        else:
            if bandwidth[i] == 0:
                load[1] = 0
                load[0] -= 1
                if load[0] >= 0:
                    workload += 1
            else:
                load[1] -= bandwidth[i]
        if load[0] <= 0 and load[1] <= 0:
            for p in job:
                server[p[0]] -= p[1]
                release += p[1]
                if len(job) > 1:
                    link[p[0]] -= 1
            if ps != -1:
                link[ps] -= 1
            ce.append([load[2], load[3]/load[2]])
        else:
            job_trace.put(job)
            job_load.put(load)
            job_ps.put(ps)
    return release, workload


def linear_evaluation(server, link, job_trace, job_ps):
    m = job_trace.qsize()
    n = len(server)
    bd = []
    for x in range(m):
        bd.append([0,1])
    c = np.array([-1 for x in range(m)])
    A_ub = [[0 for x in range(m)] for x in range(n)]
    B_ub = [1 for x in range(n)]
    A_eq = [[0 for x in range(m)]]
    B_eq = [0]
    job_link = [1 for x in range(m)]
    for i in range(m):
        job = job_trace.get()
        job_trace.put(job)
        ps = job_ps.get()
        job_ps.put(ps)
        if len(job) > 1:
            for p in job:
                A_ub[p[0]][i] += 1
                job_link[i] = max(job_link[i], link[p[0]])
        else:
            job_link[i] = 0
        if ps != -1:
            A_ub[ps][i] += 1
            job_link[i] = max(job_link[i], link[ps])
    for i in range(m):
        if job_link[i] > 0:
            tmp = [0 for x in range(m)]
            tmp[i] = -1*job_link[i]
            A_ub.append(tmp)
            B_ub.append(-1)
        else:
            tmp = [0 for x in range(m)]
            tmp[i] = 1
            A_eq.append(tmp)
            B_eq.append(0)

    res = op.linprog(c, np.array(A_ub), np.array(B_ub), np.array(A_eq), np.array(B_eq), bounds=bd)
    # print(res['x'])
    num = 0
    sums = 0.0
    for x in res['x']:
        # if x > 0:
        #     sums += x
        #     num += 1
        if x > 1:
            print("warning!")
        if x > 0:
            sums += 1/x
            num += 1
        else:
            sums += 0
            num += 1
    if num > 0:
        avg = sums/num
    else:
        avg = 0
    # print(avg)
    return avg, res['x']


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


def gpu_balance(server, link, gpus, job_trace, job_ps):
    n = len(server)
    ls = []
    flag = 0
    job = []
    tmp = []
    rest = gpus
    for i in range(n):
        tmp.append([i, server[i]])
    tmp = sorted(tmp, key=lambda x: x[1])
    # tmp.reverse()
    for i in range(n):
        usage = min(rest, PER_GPU - tmp[i][1])
        server[tmp[i][0]] += usage
        rest -= usage
        if usage > 0:
            job.append([tmp[i][0], usage])
        if usage > 0 and usage != gpus:
            link[tmp[i][0]] += 1
            flag = 1
        if rest == 0:
            break
    if flag == 1:
        for i in range(n):
            ls.append([i, link[i]])
        ls = sorted(ls, key=lambda x: x[1])
        job_ps.put(ls[0][0])
        link[ls[0][0]] += 1
    else:
        job_ps.put(-1)
    job_trace.put(job)


def Tetris(server, link, gpus, job_trace, job_ps):
    ls = []
    n = len(server)
    flag = 0
    job = []
    tmp = []
    rest = gpus
    for i in range(n):
        tmp.append([i, server[i]*gpus/PER_GPU/PER_GPU+link[i]*1])
    tmp = sorted(tmp, key=lambda x: x[1])
    for i in range(n):
        usage = min(rest, PER_GPU - server[tmp[i][0]])
        server[tmp[i][0]] += usage
        rest -= usage
        if usage > 0:
            job.append([tmp[i][0], usage])
        if usage > 0 and usage != gpus:
            link[tmp[i][0]] += 1
            flag = 1
        if rest == 0:
            break
    if flag == 1:
        for i in range(n):
            ls.append([i, link[i]])
        ls = sorted(ls, key=lambda x: x[1])
        job_ps.put(ls[0][0])
        link[ls[0][0]] += 1
    else:
        job_ps.put(-1)
    job_trace.put(job)


def Optimus(server, link, gpus, job_trace, job_ps):
    n = len(server)
    ls = []
    flag = 0
    job = []
    tmp = []
    rest = gpus
    for i in range(n):
        tmp.append([i, server[i]])
    tmp = sorted(tmp, key=lambda x: x[1])
    # tmp.reverse()
    idx = []
    for i in range(n):
        usage = min(rest, PER_GPU - tmp[i][1])
        idx.append(tmp[i])
        rest -= usage
        if rest == 0:
            break
    allocate = [0 for x in range(len(idx))]
    k = 0
    for i in range(gpus):
        while True:
            if allocate[k % len(idx)] < PER_GPU - idx[k % len(idx)][1]:
                allocate[k % len(idx)] += 1
                k += 1
                break
            else:
                k += 1
    for i in range(len(idx)):
        server[idx[i][0]] += allocate[i]
        if allocate[i] > 0:
            job.append([idx[i][0], allocate[i]])
        if 0 < allocate[i] < gpus:
            link[idx[i][0]] += 1
            flag = 1
    if flag == 1:
        for i in range(n):
            ls.append([i, link[i]])
        ls = sorted(ls, key=lambda x: x[1])
        job_ps.put(ls[0][0])
        link[ls[0][0]] += 1
    else:
        job_ps.put(-1)
    job_trace.put(job)


def link_balance(server, link, gpus, job_trace, job_ps):
    n = len(server)
    tmp = []
    ls = []
    flag = 0
    job = []
    rest = gpus
    for i in range(n):
        tmp.append([i, link[i]])
    tmp = sorted(tmp, key=lambda x: x[1])
    for i in range(n):
        usage = min(rest, PER_GPU - server[tmp[i][0]])
        server[tmp[i][0]] += usage
        rest -= usage
        if usage > 0:
            job.append([tmp[i][0], usage])
        if usage > 0 and usage != gpus:
            link[tmp[i][0]] += 1
            flag = 1
        if rest == 0:
            break
    if flag == 1:
        for i in range(n):
            ls.append([i, link[i]])
        ls = sorted(ls, key=lambda x: x[1])
        job_ps.put(ls[0][0])
        link[ls[0][0]] += 1
    else:
        job_ps.put(-1)
    job_trace.put(job)


def least_fragment(server, link, gpus, job_trace, job_ps):
    n = len(server)
    tmp = []
    ls = []
    flag = 0
    job = []
    rest = gpus
    for i in range(n):
        tmp.append([i, server[i]])
    tmp = sorted(tmp, key=lambda x: x[1])
    tmp.reverse()
    for i in range(n):
        usage = min(rest, PER_GPU - tmp[i][1])
        server[tmp[i][0]] += usage
        rest -= usage
        if usage > 0:
            job.append([tmp[i][0], usage])
        if usage > 0 and usage != gpus:
            link[tmp[i][0]] += 1
            flag = 1
        if rest == 0:
            break
    if flag == 1:
        for i in range(n):
            ls.append([i, link[i]])
        ls = sorted(ls, key=lambda x: x[1])
        job_ps.put(ls[0][0])
        link[ls[0][0]] += 1
    else:
        job_ps.put(-1)
    job_trace.put(job)


def packing(server, link, job_trace, job_ps, buffer):
    n = len(server)
    gpus = buffer.get()
    if np.sum(np.array(server)) + gpus > PER_GPU*MACHINE:
        b = buffer.qsize()
        buffer.put(gpus)
        for i in range(b):
            tmp3 = buffer.get()
            buffer.put(tmp3)
        return False, gpus
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
    # m = job_trace.qsize()
    # for i in range(m):
    #     job = job_trace.get()
    #     job_trace.put(job)
    #     ps = job_ps.get()
    #     job_ps.put(ps)
    #     if ps == -1:
    #         continue
    #     max_link = link[ps]
    #     for j in job:
    #         max_link = max(max_link, link[j[0]])
    #     for j in job:
    #         if link[j[0]] == 1 and max_link > 1:
    #             shadow_link[j[0]] = 0

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
            return True, gpus

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
    pre = np.sum(np.array(bandwidth))
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
    # print(np.sum(np.array(bandwidth)) - pre)
    if np.sum(np.array(bandwidth)) - pre < WAITING:
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
        buffer.put(gpus)
        for i in range(b):
            tmp3 = buffer.get()
            buffer.put(tmp3)
        return False, gpus
    else:
        return True, gpus


gct = []
gce = []
packing_term = 0
for i in range(1):
    job_trace = queue.Queue()
    job_load = queue.Queue()
    job_ps = queue.Queue()
    buffer = queue.Queue()
    if READ_INF:
        print("packing")
    server = [0 for x in range(MACHINE)]
    link = [0 for x in range(MACHINE)]
    gpu_sum = MACHINE * PER_GPU
    cur_sum = 0
    res = []
    term = 0
    idx = 0
    avg = 0
    bandwidth = []
    t = 0
    workload = 0
    progress = []
    comm_load = []
    band_sum = []
    ce = []
    while True:
        if idx < len(trace):
            t = trace[idx]
        if t <= 0:
            t = 1
        if idx == 50:
            k = 1
        a, b = update(server, link, job_trace, job_load, bandwidth, job_ps, ce)
        cur_sum -= a
        workload += b
        progress.append(workload)
        if cur_sum != np.sum(np.array(server)):
            k = 1
        if idx < len(trace) and cur_sum + t <= gpu_sum:
            buffer.put(t)
            idx += 1
        while buffer.qsize():
            f, g = packing(server, link, job_trace, job_ps, buffer)
            if f:
                cur_sum += g
                job_load.put([period[idx-buffer.qsize()-1], COM_TIME, 0, period[idx-buffer.qsize()-1]])
            else:
                break

        if job_trace.qsize() > 0 or buffer.qsize() > 0:
            # avg, bandwidth = linear_evaluation(server, link, job_trace, job_ps)
            avg, bandwidth, sbw = bw_evaluation(server, [], job_trace, job_ps)
            band_sum.append(np.sum(bandwidth))
            # print(link)
            # print(bandwidth)
            res.append(avg)
            cur_load = 0.0
            for i in range(job_load.qsize()):
                ps = job_ps.get()
                job_ps.put(ps)
                load = job_load.get()
                job_load.put(load)
                if ps != -1:
                    cur_load += max(load[0], 0.0) + max(load[1], 0.0)
            comm_load.append(cur_load)
        if job_trace.qsize() == 0 and buffer.qsize() == 0:
            break
        term += 1
    # if READ_INF:
    #     print(band_sum)
    jct = []
    de = []
    for x in ce:
        jct.append(x[0])
        de.append(x[1])
    gct.append(np.average(np.array(jct)))
    gce.append(np.average(np.array(de)))
    print(progress)
    # plt.plot(progress, label="packing")
    # print(server)
    # print(link)
    # print(term)
    packing_term = term
    num = 0
    sums = 0
    avg = 0
    for x in res:
        if x > 0:
            sums += x
            num += 1
    if num > 0:
        avg = sums / num
    else:
        avg = 0
    if READ_INF:
        print(avg)
    # print(job_trace)

tmp_term = 1000
for fn in [Optimus, Tetris, gpu_balance, link_balance, least_fragment]:
    job_trace = queue.Queue()
    job_load = queue.Queue()
    job_ps = queue.Queue()
    if READ_INF:
        print(fn.__name__)
    server = [0 for x in range(MACHINE)]
    link = [0 for x in range(MACHINE)]
    gpu_sum = MACHINE * PER_GPU
    cur_sum = 0
    res = []
    term = 0
    idx = 0
    avg = 0
    bandwidth = []
    t = 0
    workload = 0
    progress = []
    comm_load = []
    band_sum = []
    ce = []
    while True:
        if idx < len(trace):
            t = trace[idx]
        if t <= 0:
            t = 1
        a, b = update(server, link, job_trace, job_load, bandwidth, job_ps, ce)
        cur_sum -= a
        workload += b
        progress.append(workload)
        if idx < len(trace) and cur_sum + t <= gpu_sum:
            fn(server, link, t, job_trace, job_ps)
            job_load.put([period[idx], COM_TIME, 0, period[idx]])
            cur_sum += t
            idx += 1
        if job_trace.qsize() > 0:
            # avg, bandwidth = linear_evaluation(server, link, job_trace, job_ps)
            avg, bandwidth, sbw = bw_evaluation(server, [], job_trace, job_ps)
            band_sum.append(np.sum(bandwidth))
            # print(link)
            # print(bandwidth)
            res.append(avg)
            cur_load = 0.0
            for i in range(job_load.qsize()):
                ps = job_ps.get()
                job_ps.put(ps)
                load = job_load.get()
                job_load.put(load)
                if ps != -1:
                    cur_load += max(load[0], 0.0)+max(load[1], 0.0)
            comm_load.append(cur_load)
        if job_trace.qsize() == 0:
            break
        term += 1
    # if READ_INF:
    #     print(band_sum)
    jct = []
    de = []
    for x in ce:
        jct.append(x[0])
        de.append(x[1])
    gct.append(np.average(np.array(jct)))
    gce.append(np.average(np.array(de)))
    # plt.plot(progress, label=fn.__name__)
    # print(server)
    # print(link)
    # print(term)
    tmp_term = min(tmp_term, term)
    num = 0
    sums = 0
    avg = 0
    for x in res:
        if x > 0:
            sums += x
            num += 1
    if num > 0:
        avg = sums / num
    else:
        avg = 0
    if READ_INF:
        print(avg)
    # print(job_trace)

plt.bar([x.__name__ for x in [packing, gpu_balance, link_balance, least_fragment, Optimus, Tetris]], gct, color=['r','g','b', 'c', 'm', 'y'])
plt.xticks(rotation=45)
plt.legend()
plt.show()
for x in gct:
    print(x)
a = gct[0]
gct.remove(gct[0])
b = np.min(np.array(gct))
print((b-a)/a)