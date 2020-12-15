from Queue import PriorityQueue as PQ
import time
from SocketServer import ThreadingTCPServer, StreamRequestHandler
import socket
import threading


time_slice = 60
threshold = 60
starving = 3600
port = 8888
ban_list = [0, 1, 5, 6, 7, 8]
cfq = {}
mutex_sch = threading.Lock()
mutex_cfq = threading.Lock()



def topo(file="controller/topology", initial=False):
    f = open(file)
    s = f.readlines()
    host_num = int(s[0].strip('\n'))
    port_of_worker = []
    ip_of_worker = []
    MAC_address_of_worker = []
    for i in range(host_num):
        port_of_worker.append([int(x.strip('\n')) for x in s[i+1].split('\t')])
    for i in range(host_num):
        ip_of_worker.append([x.strip('\n') for x in s[i+1+host_num].split('\t')])
    for i in range(host_num):
        MAC_address_of_worker.append([x.strip('\n') for x in s[i+1+2*host_num].split('\t')])
    single_loopback_port = int(s[host_num*3+1].strip('\n'))

    if not initial:
        return port_of_worker, ip_of_worker, MAC_address_of_worker, single_loopback_port

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
        for j in range(0, len(port_of_worker[i])):
            p4_pd.forward_table_add_with_set_egr(
                p4_pd.forward_match_spec_t(macAddr_to_string(MAC_address_of_worker[i][j])),
                p4_pd.set_egr_action_spec_t(port_of_worker[i][j])
            )

    print("basic routine done")

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
    return port_of_worker, ip_of_worker, MAC_address_of_worker, single_loopback_port


def config(job_id, workers, ps, single_loopback_port):

    # Here we assume that one job has only one Ps
    # config outPort table
    for w in workers:
        p4_pd.outPort_table_table_add_with_set_egr_and_set_index(
            p4_pd.outPort_table_match_spec_t(
                1 << 16,
                w,  # port of worker
                0,  # if the packet is recirculate (0: no / 1: yes)
                1),  # ps index, always 1
            p4_pd.set_egr_and_set_index_action_spec_t(single_loopback_port))

    print("outPort table 1 done")

    p4_pd.outPort_table_table_add_with_set_egr(
        p4_pd.outPort_table_match_spec_t(
            1 << 16,
            single_loopback_port,
            1,
            1),
        p4_pd.set_egr_action_spec_t(ps))

    print("outPort table 2 done")

    # config drop table
    for w in workers:
        p4_pd.drop_table_table_add_with_drop_pkt(
            p4_pd.drop_table_match_spec_t(
                w,
                0)
        )

    print("drop_table done")

    # config multicast for Ps
    mcg_all = mc.mgrp_create(1000-job_id)
    node_all = mc.node_create(
        rid=1000-job_id,
        port_map=devports_to_mcbitmap(workers),
        lag_map=lags_to_mcbitmap(([]))
    )
    mc.associate_node(mcg_all, node_all, xid=0, xid_valid=False)
    conn_mgr.complete_operations()

    p4_pd.multicast_table_table_add_with_multicast(
        p4_pd.multicast_table_match_spec_t(
            1,
            1 << 16,
            ps,
            0),
        # multicast app1 -> worker1, 2
        p4_pd.multicast_action_spec_t(1000-job_id)
    )
    print("finish configuration")



class Job:
    def __init__(self, s1="", s2="", s3=""):
        self.cost = 0  # worker + ps
        self.dis = []
        self.id = 0  # not used yet
        self.waiting_time = 0
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


def send_executor(t, ip, idx, workerID=0, workerSum=0, appID=0):
    if t == "worker":
        message = "execute_worker_" + str(workerID) + "_" + str(workerSum) + "_" + str(appID)
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((ip, port))
        print("send worker request")
        client.send(message.encode('utf-8'))
        data = client.recv(1024)
        if data.decode('utf-8') == "finished!":
            if mutex_cfq.acquire():
                cfq[idx] += 1
                mutex_cfq.release()
            return
    elif t == "ps":
        message = "execute_ps"
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((ip, port))
        print("send ps request")
        client.send(message.encode('utf-8'))
        while True:
            time.sleep(5)
            if cfq[idx] == workerSum:
                cfq[idx] += 1
                message = "execute_finished"
                print("execute_finished")
                client.send(message.encode('utf-8'))
                data = client.recv(1024)
                if data.decode('utf-8') == "finished!":
                    return



class Scheduler:
    def __init__(self):
        self.job_num = 0
        self.jobs = []
        self.job_id = [0 for i in range(0, 999)]
        self.rc = 0
        self.q1 = []
        self.q2 = []
        self.dis = []
        self.worker_num = 0
        self.idx = 0
        self.port_of_worker, self.ip_of_worker, self.MAC_address_of_worker, self.single_loopback_port = topo(initial=True)
        self.worker_num = len(self.port_of_worker)
        self.workload = [0 for i in range(len(self.port_of_worker))]
        self.psload = [0 for i in range(len(self.port_of_worker))]
        for w in self.port_of_worker:
            self.dis.append([0 for i in range(len(w))])
            self.rc += len(w)
        for i in ban_list:
            self.workload[i] = len(self.dis[i])
        print("scheduler init success!")

    def read_status(self, file_status="controller/status"):
        # read status info
        f1 = open(file_status)
        s = f1.readlines()
        self.job_num = int(s[0].strip('\n'))
        self.worker_num = int(s[1].strip('\n'))
        if self.job_num == 0:
            return
        i = 0
        while i < len(s):
            self.jobs.append(Job(s[i], s[i + 1], s[i+2]))
            self.job_id[self.jobs[-1].id] = 1
            i += 3
        self.workload = [0 for i in range(0, self.worker_num)]
        self.psload = [0 for i in range(0, self.worker_num)]
        for j in self.jobs:
            for w in j.dis:
                self.dis[w[0]][w[1]] = 1
                self.workload[w[0]] += 1
            self.psload[j.dis[-1][0]] += 1
            self.rc -= j.cost

    def update(self):
        for j in self.jobs:
            j.waiting_time += time_slice

        i = 0
        while i < len(self.q2):
            if self.q2[i].waiting_time / self.q2[i].cost >= threshold or self.q2[i].waiting_time > starving:
                self.q1.append(self.q2[i])
                self.q2.pop(i)
                i -= 1
            i += 1

    def place(self, j):
        # judge new job IDs
        avail = -1  # 1000 - appID
        for i in range(1, 999):
            if self.job_id[i] == 0:
                self.job_id[i] = 1
                avail = i
                break
        if avail == -1:
            print("multicast number error")
            quit()
        idx = j.id
        j.id = avail

        assigned = 0
        pq = PQ()
        for i in range(0, self.worker_num):
            if self.workload[i] < len(self.dis[i]):
                pq.put([self.workload[i], self.psload[i], i])
        while assigned < j.cost:
            w = pq.get()
            for i in range(len(self.dis[w[2]])):
                if self.dis[w[2]][i] == 0:
                    self.dis[w[2]][i] = 1
                    j.dis.append([w[2], i])
                    self.workload[w[2]] += 1
                    w[0] += 1
                    assigned += 1
                    break
            pq.put(w)
        if assigned != j.cost:
            print("assignment error!")
            quit()
        self.rc -= j.cost
        self.psload[j.dis[-1][0]] += 1
        self.jobs.append(j)

        # install rules
        worker = [self.port_of_worker[x[0]][x[1]] for x in j.dis]
        worker.pop()
        config(j.id, worker, self.port_of_worker[j.dis[-1][0]][j.dis[-1][1]], self.single_loopback_port)

        # send message
        worker_sum = len(worker)
        threads = []
        pt = threading.Thread(target=send_executor, args=("ps", self.ip_of_worker[j.dis[-1][0]][j.dis[-1][1]], idx))
        pt.setDaemon(True)
        threads.append(pt)
        for i in range(worker_sum):
            t = threading.Thread(target=send_executor, args=("worker", self.ip_of_worker[j.dis[i][0]][j.dis[i][1]], idx, i, worker_sum, avail))
            t.setDaemon(True)
            threads.append(t)
        for t in threads:
            t.start()

        # generate config_file
        conf = open("controller/config_"+str(j.id)+"_"+str(time.time()), "w")
        conf.write(str(j.id) + '\n')
        worker_place = ""
        for p in [self.port_of_worker[x[0]][x[1]] for x in j.dis]:
            if worker_place != "":
                worker_place += '\t'
            worker_place += str(p)
        worker_place += '\n'
        conf.write(worker_place)
        conf.write(str(self.port_of_worker[j.dis[-1][0]][j.dis[-1][1]]) + '\n')
        conf.close()

    def schedule(self):
        if self.job_num > 999:
            print("overload")
            quit()

        i = 0
        while i < len(self.q1):
            if self.rc <= 0:
                break
            if self.q1[i].cost < self.rc:
                self.place(self.q1[i])
                self.q1.pop(i)
                i -= 1
            i += 1

        i = 0
        while i < len(self.q2):
            if self.rc <= 0:
                break
            if self.q2[i].cost < self.rc:
                self.place(self.q2[i])
                self.q2.pop(i)
                i -= 1
            i += 1

    def receive_job(self, j):
        self.idx += 1
        cfq[self.idx] = 0
        j.id = self.idx  # store the idx temporarily
        self.q2.append(j)
        self.schedule()
        return self.idx

    def __del__(self):
        print('controller close!')
        status = open("controller/status", "w")
        status.write(str(self.job_num) + '\n')
        status.write(str(self.worker_num) + '\n')
        for j in self.jobs:
            status.write(str(j.id) + '\n')
            status.write(str(j.waiting_time) + '\n')
            job_dis = ""
            for num in j.dis:
                if job_dis != "":
                    job_dis += '\t'
                job_dis += str(num[0])+","+str(num[1])
            job_dis += '\n'
            status.write(job_dis)


sch = Scheduler()


class myTCP(StreamRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        print("receive from (%r):%r" % (self.client_address, data.decode('utf-8')))
        s = data.decode('utf-8').split("_")
        idx = -1
        if s[0] == "control":
            if mutex_sch.acquire():
                job = Job()
                job.cost = int(s[1])
                idx = sch.receive_job(job)
                mutex_sch.release()
                while True:
                    time.sleep(10)
                    print(cfq.get(idx, -1))
                    print(job.cost)
                    if cfq.get(idx, -1) == job.cost:  # check cfq
                        print("finished!")
                        self.wfile.write("finished!".encode('utf-8'))
                        break
        else:
            print("error message!")
            return


if __name__ == '__main__':
    ip = "10.0.0.10"
    print(ip)
    addr = (ip, port)
    print("server up")
    server = ThreadingTCPServer(addr, myTCP)
    server.serve_forever()
