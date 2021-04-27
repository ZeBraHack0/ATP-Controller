import queue
import numpy as np
from scipy import optimize as op
import matplotlib.pyplot as plt
import sys
import math

MACHINE = 10
PER_GPU = 2
DBL_MIN = float('-Infinity')
JOB_TIME = 5
COM_TIME = 0
READ_INF = 0

# trace = [6,7,8,9,10,5,5]
# trace = [6,6,8,9,10,5,6,6,6,8,7,6,5,6,6,6,9,8,7,6,5,6,4,2,3,6,7,4,2,3,9,9,8,7,6,5,6,6,7,8,9,10,5,]
# trace = [9,9,8,7,6,5,6]
# trace = [4,2,3,9,9,8,7,6,5,6,6,7,8,9,10,5,2,4,6,7,8,9,2,10,5,7,6,7,8,9,10,5,5,6,9,8,7,6,5,6,4,2,1,3,2,4,6,5,8]
# trace = [9,9,8,7,6,5,6,4,2,3,6,7,8,9,2,10,5,5,6,8,7,6,5,6,8,7,6,5,6,4,2,3,6,8,9,10,5,4,2,3,9,9,8,7]
# trace = [16, 8, 4, 4, 8, 8, 8, 2, 2, 4, 2, 2, 2, 4, 8, 8, 4, 2, 4, 2, 2, 2, 8, 8, 2, 8, 4, 2, 2, 8, 2, 2, 2, 8, 8, 2, 4, 8, 8, 4, 8, 8, 2, 4, 4, 2, 4, 2, 2, 4, 8, 8, 2, 4, 2, 8, 2, 4, 8]
# trace = np.random.poisson(PER_GPU, 50)
trace = np.random.zipf(2, 50)
print(trace)


def update(server, link, job_trace, job_load, bandwidth, job_ps):
    release = 0
    workload = 0
    for i in range(len(bandwidth)):
        job = job_trace.get()
        load = job_load.get()
        ps = job_ps.get()
        if load[0] > 0 and load[1] <= 0:
            load[0] -= 1
            if load[0] >= 0:
                workload += 1
            load[1] = 1
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
    A_eq = [[0 for x in range(m)] for x in range(n)]
    B_eq = [0 for x in range(n)]
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
        if x > 0:
            sums += x
            num += 1
        else:
            sums += JOB_TIME
            num += 1
    if num > 0:
        avg = sums/num
    else:
        avg = 0
    # print(avg)
    return avg, res['x']


def evaluation(server, link):
    n = 0
    bw = 0
    for w in link:
        if w > 0:
            bw += 1
            n += w
    # print(bw/n)


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


def ATP_balance(server, link, gpus, job_trace, job_ps):
    n = len(server)
    ls = []
    flag = 0
    if gpus <= PER_GPU:
        for i in range(n):
            if PER_GPU - server[i] >= gpus:
                flag = 1
                break
    if flag == 0:
        for i in range(n):
            ls.append([i, link[i]])
        ls = sorted(ls, key=lambda x: x[1])
        job_ps.put(ls[0][0])
        link[ls[0][0]] += 1
    else:
        job_ps.put(-1)
    map = {}
    job = []
    rest = gpus
    for i in range(n):
        if PER_GPU-server[i] in map:
            map[PER_GPU-server[i]].put(i)
        else:
            map[PER_GPU-server[i]] = queue.Queue()
            map[PER_GPU - server[i]].put(i)

    # one division
    if gpus <= PER_GPU:
        # ATP_balance(server, link, gpus, job_trace)
        # return
        min_del = sys.maxsize
        idx = -1
        for i in range(n):
            if PER_GPU - server[i] >= gpus and min_del > PER_GPU - server[i] - gpus:
                min_del = PER_GPU - server[i] - gpus
                idx = i
        if idx != -1:
            server[idx] += gpus
            job_trace.put([[idx, gpus]])
            return


    # two division
    for a in range(1, int(rest/2)+1):
        b = rest - a
        if a in map and b in map:
            if a == b and map[a].qsize() < 2:
                break
            aidx = map[a].get()
            server[aidx] += a
            link[aidx] += 1
            job.append([aidx, a])
            bidx = map[b].get()
            server[bidx] += b
            link[bidx] += 1
            job.append([bidx, b])
            job_trace.put(job)
            return
    gpu_balance(server, link, rest, job_trace)


def packing(server, link, gpus, job_trace, job_ps):
    n = len(server)
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
    m = job_trace.qsize()
    for i in range(m):
        job = job_trace.get()
        job_trace.put(job)
        ps = job_ps.get()
        job_ps.put(ps)
        if ps == -1:
            continue
        max_link = link[ps]
        for j in job:
            max_link = max(max_link, link[j[0]])
        for j in job:
            if link[j[0]] == 1 and max_link > 1:
                shadow_link[j[0]] = 0

    # print(link)
    ls = []
    flag = 0
    # connectionless solution
    if gpus <= PER_GPU:
        # ATP_balance(server, link, gpus, job_trace)
        # return
        min_del = sys.maxsize
        min_link = sys.maxsize
        idx = -1
        for i in range(n):
            if PER_GPU - server[i] >= gpus and (min_del > PER_GPU - server[i] - gpus or (min_del == PER_GPU - server[i] - gpus and min_link > link[i])):
                min_del = PER_GPU - server[i] - gpus
                min_link = link[i]
                idx = i
        if idx != -1:
            server[idx] += gpus
            job_trace.put([[idx, gpus]])
            job_ps.put(-1)
            return [idx, gpus], -1

    # connection-oriented solution

    dp = [[DBL_MIN for x in range(gpus+PER_GPU+1)] for j in range(up_link+1)]
    trace = [[[[-1, -1] for x in range(gpus+PER_GPU+1)]for x in range(n)]for j in range(up_link+1)]
    for i in range(up_link+1):
        dp[i][0] = 1/(i+1)
    for i in range(n):
        w = PER_GPU - server[i]
        # v = -1 * (1 - 1 / (shadow_link[i] + 1))
        v = 0
        if shadow_link[i] > 0:
            v = -shadow_link[i]*(1/shadow_link[i]-1/(shadow_link[i]+1))
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
            a = dp[s][gpus]
            ans = gpus
            # level = int(math.log(dp[gpus]*-1, 100))
            level = dp[s][gpus]
            state = s
    if ans == -1 or empty:
        for s in range(up_link + 1):
            for i in range(gpus+1, gpus+PER_GPU+1):
                if dp[s][i] == DBL_MIN:
                    continue
                # tmp = int(math.log(dp[i]*-1, 100))
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
        ls.append([i, link[i]])
    ls = sorted(ls, key=lambda x: x[1])
    job_ps.put(ls[0][0])
    # print(ls[0][1])
    link[ls[0][0]] += 1
    job_trace.put(job)
    return job, ls[0][0]


if __name__ == "__main__":
    if True:
        job_trace = queue.Queue()
        job_load = queue.Queue()
        job_ps = queue.Queue()
        if READ_INF:
            print("gpu_balance")
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
        while True:
            if idx < len(trace):
                t = trace[idx]
            a, b = update(server, link, job_trace, job_load, bandwidth, job_ps)
            cur_sum -= a
            workload += b
            progress.append(workload)
            if idx < len(trace) and cur_sum + t <= gpu_sum:
                gpu_balance(server, link, t, job_trace, job_ps)
                job_load.put([JOB_TIME, COM_TIME])
                cur_sum += t
                idx += 1
            if job_trace.qsize() > 0:
                avg, bandwidth = linear_evaluation(server, link, job_trace, job_ps)
                res.append(avg)
            if job_trace.qsize() == 0:
                break
            term += 1
        plt.plot(progress, label="gpu_balance")
        # print(server)
        # print(link)
        print(term)
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
    if True:
        job_trace = queue.Queue()
        job_load = queue.Queue()
        job_ps = queue.Queue()
        if READ_INF:
            print("link_balance")
        server = [0 for x in range(MACHINE)]
        link = [0 for x in range(MACHINE)]
        gpu_sum = MACHINE * PER_GPU
        cur_sum = 0
        res = []
        term = 0
        idx= 0
        avg = 0
        bandwidth = []
        t = 0
        workload = 0
        progress = []
        while True:
            if idx < len(trace):
                t = trace[idx]
            a, b = update(server, link, job_trace, job_load, bandwidth, job_ps)
            cur_sum -= a
            workload += b
            progress.append(workload)
            if idx < len(trace) and cur_sum + t <= gpu_sum:
                link_balance(server, link, t, job_trace, job_ps)
                job_load.put([JOB_TIME, COM_TIME])
                cur_sum += t
                idx += 1
            if job_trace.qsize() > 0:
                avg, bandwidth = linear_evaluation(server, link, job_trace, job_ps)
                res.append(avg)
            if job_trace.qsize() == 0:
                break
            term += 1
        plt.plot(progress, label="link_balance")
        # print(server)
        # print(link)
        print(term)
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
    if True:
        job_trace = queue.Queue()
        job_load = queue.Queue()
        job_ps = queue.Queue()
        if READ_INF:
            print("least_fragment")
        server = [0 for x in range(MACHINE)]
        link = [0 for x in range(MACHINE)]
        gpu_sum = MACHINE * PER_GPU
        cur_sum = 0
        res = []
        term = 0
        idx= 0
        avg = 0
        bandwidth = []
        t = 0
        workload = 0
        progress = []
        while True:
            if idx < len(trace):
                t = trace[idx]
            a, b = update(server, link, job_trace, job_load, bandwidth, job_ps)
            cur_sum -= a
            workload += b
            progress.append(workload)
            if idx < len(trace) and cur_sum + t <= gpu_sum:
                least_fragment(server, link, t, job_trace, job_ps)
                job_load.put([JOB_TIME, COM_TIME])
                cur_sum += t
                idx += 1
            if job_trace.qsize() > 0:
                avg, bandwidth = linear_evaluation(server, link, job_trace, job_ps)
                res.append(avg)
            if job_trace.qsize() == 0:
                break
            term += 1
        plt.plot(progress, label="least_fragment")
        # print(server)
        # print(link)
        print(term)
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
    if True:
        job_trace = queue.Queue()
        job_load = queue.Queue()
        job_ps = queue.Queue()
        if READ_INF:
            print("Optimus")
        server = [0 for x in range(MACHINE)]
        link = [0 for x in range(MACHINE)]
        gpu_sum = MACHINE * PER_GPU
        cur_sum = 0
        res = []
        term = 0
        idx= 0
        avg = 0
        bandwidth = []
        t = 0
        workload = 0
        progress = []
        while True:
            if idx < len(trace):
                t = trace[idx]
            a, b = update(server, link, job_trace, job_load, bandwidth, job_ps)
            cur_sum -= a
            workload += b
            progress.append(workload)
            if idx < len(trace) and cur_sum + t <= gpu_sum:
                Optimus(server, link, t, job_trace, job_ps)
                job_load.put([JOB_TIME, COM_TIME])
                cur_sum += t
                idx += 1
            if job_trace.qsize() > 0:
                avg, bandwidth = linear_evaluation(server, link, job_trace, job_ps)
                res.append(avg)
            if job_trace.qsize() == 0:
                break
            term += 1
        plt.plot(progress, label="Optimus")
        # print(server)
        # print(link)
        print(term)
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
    if True:
        job_trace = queue.Queue()
        job_load = queue.Queue()
        job_ps = queue.Queue()
        if READ_INF:
            print("Tetris")
        server = [0 for x in range(MACHINE)]
        link = [0 for x in range(MACHINE)]
        gpu_sum = MACHINE * PER_GPU
        cur_sum = 0
        res = []
        term = 0
        idx= 0
        avg = 0
        bandwidth = []
        t = 0
        workload = 0
        progress = []
        while True:
            if idx < len(trace):
                t = trace[idx]
            a, b = update(server, link, job_trace, job_load, bandwidth, job_ps)
            cur_sum -= a
            workload += b
            progress.append(workload)
            if idx < len(trace) and cur_sum + t <= gpu_sum:
                # print(server, link)
                Tetris(server, link, t, job_trace, job_ps)
                # print(server, link)
                job_load.put([JOB_TIME, COM_TIME])
                cur_sum += t
                idx += 1
            if job_trace.qsize() > 0:
                avg, bandwidth = linear_evaluation(server, link, job_trace, job_ps)
                res.append(avg)
            if job_trace.qsize() == 0:
                break
            term += 1
        plt.plot(progress, label="Tetris")
        # print(server)
        # print(link)
        print(term)
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
    # if True:
    #     job_trace = queue.Queue()
    #     job_load = queue.Queue()
    #     print("ATP_balance")
    #     server = [0 for x in range(MACHINE)]
    #     link = [0 for x in range(MACHINE)]
    #     gpu_sum = MACHINE * PER_GPU
    #     cur_sum = 0
    #     res = []
    #     term = 0
    #     idx= 0
    #     avg = 0
    #     bandwidth = []
    #     t = 0
    #     workload = 0
    #     progress = []
    #     while True:
    #         if idx < len(trace):
    #             t = trace[idx]
    #         a, b = update(server, link, job_trace, job_load, bandwidth, job_ps)
    #         cur_sum -= a
    #         workload += b
    #         progress.append(workload)
    #         if idx < len(trace) and cur_sum + t <= gpu_sum:
    #             ATP_balance(server, link, t, job_trace)
    #             job_load.put([JOB_TIME, COM_TIME])
    #             cur_sum += t
    #             idx += 1
    #         if job_trace.qsize() > 0:
    #             avg, bandwidth = linear_evaluation(server, link, job_trace, job_ps)
    #             res.append(avg)
    #         if job_trace.qsize() == 0:
    #             break
    #         term += 1
    #     plt.plot(progress, label="ATP_balance")
    #     # print(server)
    #     # print(link)
    #     print(term)
    #     num = 0
    #     sums = 0
    #     avg = 0
    #     for x in res:
    #         if x > 0:
    #             sums += x
    #             num += 1
    #     if num > 0:
    #         avg = sums / num
    #     else:
    #         avg = 0
    #     print(avg)
    #     # print(job_trace)
    if True:
        job_trace = queue.Queue()
        job_load = queue.Queue()
        job_ps = queue.Queue()
        if READ_INF:
            print("packing")
        server = [0 for x in range(MACHINE)]
        link = [0 for x in range(MACHINE)]
        gpu_sum = MACHINE * PER_GPU
        cur_sum = 0
        res = []
        term = 0
        idx= 0
        avg = 0
        bandwidth = []
        t = 0
        workload = 0
        progress = []
        while True:
            if idx < len(trace):
                t = trace[idx]
            a, b = update(server, link, job_trace, job_load, bandwidth, job_ps)
            cur_sum -= a
            workload += b
            progress.append(workload)
            if idx < len(trace) and cur_sum + t <= gpu_sum:
                # print(server, link)
                packing(server, link, t, job_trace, job_ps)
                # print(server, link)
                job_load.put([JOB_TIME, COM_TIME])
                cur_sum += t
                idx += 1
            if job_trace.qsize() > 0:
                avg, bandwidth = linear_evaluation(server, link, job_trace, job_ps)
                res.append(avg)
            if job_trace.qsize() == 0:
                break
            term += 1
        plt.plot(progress, label="packing", color="r")
        # print(server)
        # print(link)
        print(term)
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

    plt.legend()
    plt.show()