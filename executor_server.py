from SocketServer import ThreadingTCPServer, StreamRequestHandler
import threading
import os
import subprocess
import socket

sudoPassword = 'zbh'


class myTCP(StreamRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        print("receive from (%r):%r" % (self.client_address, data.decode('utf-8')))
        s = data.decode('utf-8').split("_")
        idx = -1
        if s[0] == "execute":
            if s[1] == "worker":
                workerID = s[2]
                workerSum = s[3]
                appID = s[4]
                cmd = "sudo ~/ATP_NSDI21-master/atp/client/app " + workerID + " " + workerSum + " " + appID + " " + "1"
                os.system('echo %s|sudo -S %s' % (sudoPassword, cmd))
                self.wfile.write("finished!".encode('utf-8'))
                print("finished!")
                return
            if s[1] == "ps":
                cmd = "sudo ~/ATP_NSDI21-master/atp/server/app"
                proc = subprocess.Popen(cmd, shell=True)
                proc.communicate(sudoPassword.encode('utf-8'))
                data2 = self.request.recv(1024)
                print(data2.decode('utf-8'))
                if data2.decode('utf-8') == "execute_finished":
                    self.wfile.write("finished!".encode('utf-8'))
                    print("finished!")
                    # proc.terminate()
                else:
                    print("error message!")
                    self.wfile.write("error message!".encode('utf-8'))
                return
        else:
            print("error message!")
            return


if __name__ == '__main__':
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    print(ip)
    port = 8888
    addr = (ip, port)
    print("server up")
    server = ThreadingTCPServer(addr, myTCP)
    server.serve_forever()