from socket import *
import threading
import time

# gpus = [3, 1, 2, 1, 2, 1, 5, 4, 4, 1, 2]
gpus = [3, 1, 2, 1, 2, 1, 5]
models = ['vgg16' for x in range(len(gpus))]
dataset = ['benchmark' for y in range(len(gpus))]
iteration = [10 for z in range(len(gpus))]
cnt = 0
mutex = threading.Lock()


def inject(idx, redundant):
    host = '10.0.0.10'
    port = 8888
    bufsize = 1024
    client = socket(AF_INET, SOCK_STREAM)
    client.connect((host, port))
    print("client up, enter your job")

    try:
        cmd = "control_" + str(gpus[idx]) + "_" + dataset[idx] + "_" + models[idx] + "_" + str(iteration[idx])
        client.send(cmd.encode('utf-8'))
        print("submit job " + str(idx) + " successfully!")
        print("waiting for job " + str(idx) + " finishing...")
        data = client.recv(bufsize)
        print(data.decode('utf-8'))
    except Exception as e:
        print(e)
    if mutex.acquire():
        global cnt
        cnt += 1
        mutex.release()
    client.close()


threads = []
for i in range(len(gpus)):
    th = threading.Thread(target=inject, args=(i, 0))
    th.setDaemon(True)
    threads.append(th)
    time.sleep(1)  # to guarantee order

begin_time = time.time()
while True:
    time.sleep(1)
    global cnt
    print("current finished jobs:", cnt)
    if cnt >= len(gpus):
        break
end_time = time.time()
run_time = end_time-begin_time
print('runtime:', run_time)
f = open("injection.txt", "w")
f.write(str(run_time))