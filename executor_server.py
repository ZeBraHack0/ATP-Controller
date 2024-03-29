from SocketServer import ThreadingTCPServer, StreamRequestHandler
import threading
import os
import subprocess
import socket
import nvgpu

sudoPassword = 'zbh'
agtr = 20000
win = 250
threadNum = 12
# "P4ML_MAX_AGTR_PER_THREAD="
# "P4ML_HASH_RANGE"


print(str(len(nvgpu.available_gpus())) + " is available!")
class myTCP(StreamRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        print("receive from (%r):%r" % (self.client_address, data.decode('utf-8')))
        s = data.decode('utf-8').split("_")
        idx = -1
        cmd = ""
        if s[0] == "execute":
            if s[1] == "worker":
                cmd += "P4ML_MAX_AGTR_PER_THREAD=" + str(win) + " "
                cmd += "P4ML_HASH_RANGE=" + str(agtr) + " "
                cmd += "P4ML_THREAD=" + str(threadNum) + " "
                workerID = s[2]
                cmd += "DMLC_WORKER_ID=" + workerID + " "
                workerSum = s[3]
                cmd += "DMLC_NUM_WORKER=" + workerSum + " "
                appID = s[4]
                cmd += "P4ML_APP=" + appID + " "
                gpu = s[5]
                if len(gpu) > 1:
                    cmd += "NVIDIA_VISIBLE_DEVICES=" + gpu + " "
                else:
                    cmd += "CUDA_VISIBLE_DEVICES=" + gpu + " "
                dataset = s[6]
                cmd += "EVAL_TYPE=" + dataset + " "
                model = s[7]
                iter_num = s[8]
                # other interface to open: dataset, model, iter_num
                cmd = "sudo -E " + cmd + "DMLC_ROLE=worker DMLC_NUM_SERVER=1  DMLC_INTERFACE=enp178s0f0 DMLC_PS_ROOT_URI=192.168.0.3 DMLC_PS_ROOT_PORT=6767 python ~/ATP-Controller/launcher/launch.py python ~/ATP-Controller/example/pytorch/benchmark_byteps.py --model " + model + " --num-iters " + iter_num
                print(cmd)

                os.system('echo %s|sudo -S %s' % (sudoPassword, cmd))
                self.wfile.write("finished!".encode('utf-8'))
                print("finished!")
                return
            if s[1] == "ps":
                cmd = "sudo ~/ATP-Controller/server/app " + s[2] + " " + str(agtr) + " " + str(win) + " " + str(threadNum)
                print(cmd)
                os.system('echo %s|sudo -S %s' % (sudoPassword, cmd))
                # data2 = self.request.recv(1024)
                # if data2.decode('utf-8') == "execute_finished":
                #     self.wfile.write("finished!".encode('utf-8'))
                #     print("finished!")
                #     # proc.terminate()
                # else:
                #     print("error message!")
                #     self.wfile.write("error message!".encode('utf-8'))
                return
        elif s[0] == "gpu":
            s = str(len(nvgpu.available_gpus()))
            self.wfile.write(s.encode('utf-8'))
            print(s + " is available!")
            return
        else:
            print("error message!")
            return


if __name__ == '__main__':
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    ip = '10.0.0.' + ip[-1]
    print(ip)
    port = 8888
    addr = (ip, port)
    print("server up")
    server = ThreadingTCPServer(addr, myTCP)
    server.serve_forever()