import os
import subprocess
import time
from socket import *
import argparse
from Queue import PriorityQueue as PQ
from SocketServer import ThreadingTCPServer, StreamRequestHandler
import socket
import threading
mutex_sch = threading.Lock()

ip_of_worker = {"56":"10.0.0.1", "48":"10.0.0.2", "40":"10.0.0.3", 
                "32":"10.0.0.4", "24" : "10.0.0.5", "16":"10.0.0.6", 
                "8":"10.0.0.7", "0":"10.0.0.8", "4": "10.0.0.9"}



def hex_to_i32(h):
    x = int(h, 0)
    if (x > 0xFFFFFFFF):
        raise UIn_Error("Integer cannot fit within 32 bits")
    if (x > 0x7FFFFFFF): x-= 0x100000000
    return x

def hex_to_i16(h):
    x = int(h, 0)
    if (x > 0xFFFF):
        raise UIn_Error("Integer cannot fit within 16 bits")
    if (x > 0x7FFF): x-= 0x10000
    return x

class Job:
    def __init__(self, cost=1, dataset="", model="", num_iters=10, is_MDJ=0, workers=[], ps=[]):
        self.cost = cost  # gpu num
        self.id = 0
        self.dataset = dataset
        self.model = model
        self.num_iters = num_iters
        self.waiting_time = 0
        self.is_MDJ = is_MDJ
        self.workers = workers
        self.ps = ps
        

    def set(self, id, wt, ds):
        self.id = id
        self.waiting_time = wt
        self.dis = ds
        self.cost = len(self.dis)

class Controller:
    def __init__(self):
        self.job_num = 0
        self.jobs = []
        self.general_job = []
        self.MDJ_job = []
        # self.basic_setup()
        
        
    
    print("scheduler init success!")
    
    def basic_setup():
        cmd = "python ~/bf-sde-8.9.1/run_pd_rpc.py --thrift-ip 10.0.0.10 -p p4ml_lc_test"
        # os.system(cmd)
        ts = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
        ts.stdin.write("SCRIPT_DIR='/home/yiyi/yiyi/'\n".encode())
        cmd1 = "execscript('" + "setup.py" + "')\n"
        ts.stdin.write(cmd1.encode())
        ts.stdin.close()
        std_return = ts.stdout.read()
        print(std_return.decode())
        std_error = ts.stderr.read()
        print(std_error.decode())

    def config(job_id, workers, ps, single_loopback_port):
        single_loopback_port = bytes(single_loopback_port)
        job_id = bytes(job_id)
        ps=bytes(ps)

        cmd = "python ~/bf-sde-8.9.1/run_pd_rpc.py --thrift-ip 10.0.0.10 -p p4ml_lc_test"
        ts = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
        # ts.stdin.write("SCRIPT_DIR='/home/yiyi/'\n".encode())
        
        cmd_table_match = "p4_pd.outPort_table_table_add_with_set_egr_and_set_index(p4_pd.outPort_table_match_spec_t("
        cmd_action = "p4_pd.set_egr_and_set_index_action_spec_t("
        # Here we assume that one job has only one Ps
        # config outPort table
        for w in workers:
            cmd_config = cmd_table_match + job_id + " << 16,\n" + bytes(w) + ", 0, 0)," + cmd_action + single_loopback_port + "))\n"
            ts.stdin.write(cmd_config.encode())
        
        print("outPort table 1 done")


        cmd_table_match = "p4_pd.outPort_table_table_add_with_set_egr(" + "p4_pd.outPort_table_match_spec_t("
        cmd_action = "p4_pd.set_egr_action_spec_t("
        cmd_config_2 = cmd_table_match + job_id + "<< 16," + single_loopback_port + ",1,0)," + cmd_action + ps + "))\n"
        ts.stdin.write(cmd_config_2.encode())

        print("outPort table 2 done")

        cmd_table_match = "p4_pd.drop_table_table_add_with_drop_pkt(p4_pd.drop_table_match_spec_t("
        
        # config drop table
        for w in workers:
            cmd_config_3 = cmd_table_match + bytes(w) + ", 0))\n"
            ts.stdin.write(cmd_config_3.encode())
        
        print("drop_table done")

        # config multicast for Ps
        
        ###### Server ########
        cmd_multicast = "mcg_all = mc.mgrp_create(1000 - "+ job_id +")\nnode_all = mc.node_create(rid=1000-" + job_id+ ",port_map=devports_to_mcbitmap("
        cmd_multicast_node = bytes(workers_in_used) + "), lag_map=lags_to_mcbitmap(([])))\nmc.associate_node(mcg_all, node_all, xid=0, xid_valid=False)\n"
        cmd_complete_opreation = "conn_mgr.complete_operations()\n"
        ts.stdin.write(cmd_multicast + cmd_multicast_node + cmd_complete_opreation)
        print(cmd_multicast + cmd_multicast_node + cmd_complete_opreation)
        
        

        cmd_table_match = "p4_pd.multicast_table_table_add_with_multicast(p4_pd.multicast_table_match_spec_t(1," + job_id + " << 16," + ps + ",0),"
            # multicast app1 -> worker1, 2
        cmd_action = "p4_pd.multicast_action_spec_t(1000-" + job_id + "))\n"
        cmd_config_4 = cmd_table_match + cmd_action
        ts.stdin.write(cmd_config_4)
        print("finish configuration")
        print(cmd_config_4)

        ts.stdin.close()
        print(ts.stdout.read().decode())
        print(ts.stderr.read().decode())
        ts.stdout.close()
        ts.stderr.close()

    def deconfig(job_id, workers, ps, single_loopback_port):
        single_loopback_port = bytes(single_loopback_port)
        job_id = bytes(job_id)
        ps=bytes(ps)
        
        cmd = "python ~/bf-sde-8.9.1/run_pd_rpc.py --thrift-ip 10.0.0.10 -p p4ml_lc_test"
        ts = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
        
        cmd_table_match = "p4_pd.outPort_table_table_delete_by_match_spec(p4_pd.outPort_table_match_spec_t("
        # deconfig outPort table
        for w in workers:
            cmd_config = cmd_table_match + job_id + " << 16," + bytes(w) + ", 0, 0))\n"
            ts.stdin.write(cmd_config.encode())
        
        # print("outPort table 1 done")
        
        cmd_table_match = "p4_pd.outPort_table_table_delete_by_match_spec(p4_pd.outPort_table_match_spec_t("
        cmd_config = cmd_table_match + job_id + " << 16," + bytes(single_loopback_port) + ", 1, 0))\n"
        ts.stdin.write(cmd_config.encode())
        
        print("outPort table 2 deleted")

        # config drop table
        cmd_table_match = "p4_pd.drop_table_table_delete_by_match_spec(p4_pd.drop_table_match_spec_t("
        for w in workers:
            cmd_config = cmd_table_match + bytes(w) + ",0))\n"
            ts.stdin.write(cmd_config.encode())
            
        

        print("drop_table deleted")

        # config multicast for Ps
        cmd_table_match = "p4_pd.multicast_table_table_delete_by_match_spec(p4_pd.multicast_table_match_spec_t(1,"
        cmd_config = cmd_table_match +  job_id + "<< 16," + ps + ",0))\n"
        ts.stdin.write(cmd_config)
        print(cmd_config)
        print("finish deletion")
        
        ts.stdin.close()
        print(ts.stdout.read().decode())
        print(ts.stderr.read().decode())
        ts.stdout.close()
        ts.stderr.close()

    def config_match(job_id, version, action_mod, action_add, action_loop_add, action_version_add):
        job_id = bytes(job_id)
        version = bytes(version)
        action_mod = hex_to_i16(bytes(action_mod))
        action_add = hex_to_i16(bytes(action_add))
        
        action_loop_add = hex_to_i16(bytes(action_loop_add))
        action_version_add = hex_to_i16(bytes(action_version_add))

        cmd = "python ~/bf-sde-8.9.1/run_pd_rpc.py --thrift-ip 10.0.0.10 -p p4ml_lc_test"
        ts = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
        
        
        cmd_match = "p4_pd.match_appID_table_add_with_set_new_seq(p4_pd.match_appID_match_spec_t("
        cmd_action = "p4_pd.set_new_seq_action_spec_t("
        ts.stdin.write(cmd_match + job_id + "<<16," + version + ")," + cmd_action + bytes(action_mod) + "," + bytes(action_add) + ","
                    + bytes(action_loop_add) + "," + bytes(action_version_add) + "))")
        print(cmd_match + job_id + "<<16," + version + ")," + cmd_action + bytes(action_mod) + "," + bytes(action_add) + ","
                    + bytes(action_loop_add) + "," + bytes(action_version_add) + "))")

        ts.stdin.close()
        print(ts.stdout.read().decode())
        print(ts.stderr.read().decode())
        ts.stdout.close()
        ts.stderr.close()


    def receive_job(self, job):
        self.job_num += 1
        job.id = self.job_num
        if (job.is_MDJ == 0):
            self.general_job.append(job)
        else:
            self.MDJ_job.append(job)
        self.place(job)

    

    def place(self, job):
        
        # TODO: add placement policy
        if (job.is_MDJ == 1):
            config(job_id=job_id, workers=workers_in_used, ps=ps, single_loopback_port=single_loopback_port)
    
        # deconfig(job_id=job_id, workers=workers_in_used, ps=ps, single_loopback_port=single_loopback_port)
        config_match(1, 0, 0x7FFF, 0x0000, 0x4E20, 0x0001)
        config_match(1, 1, 0X7FFF, 0x0000, 0x4E20, 0x0000)


        # send message
        worker_sum = len(job.workers)
        threads = []
        if len(job.ps) >= 1:  # need a ps
            pt = threading.Thread(target=send_executor, args=("ps", ip_of_worker, 0, worker_sum, job))
            pt.setDaemon(True)
            threads.append(pt)
        for i in range(worker_sum):
            th = threading.Thread(target=send_executor, args=("worker", ip_of_worker, i, worker_sum, job))
            th.setDaemon(True)
            threads.append(th)
        for th in threads:
            th.start()


def send_executor(role, ip_of_worker, workerID, workerSum, tar_job):
    appID = tar_job.id
    gpu_num = 0
    
    port=8889
    
    
    if role == "worker":
        # destination ip
        des = ip_of_worker[str(tar_job.workers[workerID])]
        print(des)

        message = "execute_worker_" + str(workerID) + "_" + str(workerSum) + "_" + str(appID) + "_"
        message += str(tar_job.cost)
        message += ","
        gpu_num += 1
        message = message.strip(",")
        message += "_" + tar_job.dataset + "_" + tar_job.model + "_" + str(tar_job.num_iters)
        print(message)

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # handshaking
        flag = 0
        while flag == 0:
            time.sleep(1)
            try:
                client.connect((des, port))
                print("worker " + str(workerID) + " connected")
                flag = 1
            except Exception as e:
                print(e)

        print("send worker request")
        client.send(message.encode('utf-8'))
        data = client.recv(1024)
        if data.decode('utf-8') == "finished!":
            print("workerID : "+ str(workerID) + " finished!")
            return

    elif role == "ps":
        message = "execute_ps_" + str(tar_job.id) 
        des = ip_of_worker[str(tar_job.ps[workerID])]
        print("ps : ")
        print(des)
        print(message)
        
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # handshaking
        flag = 0
        while flag == 0:
            time.sleep(1)
            try:
                client.connect((des, port))
                print("server connected")            
                flag = 1
            except Exception as e:
                print(e)

        print("send ps request")
        client.send(message.encode('utf-8'))
        data = client.recv(1024)
        if data.decode('utf-8') == "finished!":
            print("ps finished")
            client.close()
            return
        
        # while True:
        #     time.sleep(5)
        #     client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     client.connect((des, 10000+appID))
        #     message = "execute_finished"
        #     client.send(message.encode('utf-8'))
        #     client.close()
            
        #     print("execute_finished")
        #     # client.send(message.encode('utf-8'))
        #     # data = client.recv(1024)
        #     # if data.decode('utf-8') == "finished!":
        #     return



class myTCP(StreamRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        print("receive from (%r):%r" % (self.client_address, data.decode('utf-8')))
        s = data.decode('utf-8').split("_")
        idx = -1   
        
        if s[0] == "control":
            if mutex_sch.acquire():
                is_MDJ = int(s[5])
                workers_in_used = eval(s[6])
                worker_sum = len(workers_in_used)
                ps = eval(s[7])
                job = Job(cost=int(s[1]),dataset=s[2], model=s[3], num_iters=int(s[4]), is_MDJ=is_MDJ, workers=workers_in_used, ps=ps)
                
                # send_executor("ps", ip_of_worker, 0, worker_sum, job)
                # for i in range(worker_sum):
                #     send_executor("worker", ip_of_worker, i, worker_sum, job)
                    
                idx = controller.receive_job(job)
                mutex_sch.release()



# def run_controller():
#     while True:
#         time.sleep(5)
#         if mutex_sch.acquire():

#             print("get mutx and release")
#             mutex_sch.release()

controller = Controller()

def main():
    ip = "10.0.0.8"
    port=8888
    print(ip)
    addr = (ip, port)
    print("server up")
    
    
    # run_controller()

    server = ThreadingTCPServer(addr, myTCP)
    server.serve_forever()
    
    # basic_setup()
    # config(job_id=job_id, workers=workers_in_used, ps=ps, single_loopback_port=single_loopback_port)
    # # # time.sleep(5)
    # deconfig(job_id=job_id, workers=workers_in_used, ps=ps, single_loopback_port=single_loopback_port)
    # config_match(1, 0, 0x7FFF, 0x0000, 0x4E20, 0x0001)
    # config_match(1, 1, 0X7FFF, 0x0000, 0x4E20, 0x0000)

if __name__ == "__main__":
    main()
