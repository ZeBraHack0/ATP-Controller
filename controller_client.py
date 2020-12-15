from socket import *

host = '10.0.0.10'
port = 8888
bufsize = 1024
addr = (host, port)
client = socket(AF_INET, SOCK_STREAM)
client.connect((host, port))
print("client up, enter your job")


data = input()
if not data or data == 'exit':
    exit()
try:
    w = int(data)
    cmd = "control_" + str(data)
    client.send(cmd.encode('utf-8'))
    print("submit job successfully!")
    print("waiting for job finishing...")
    data = client.recv(bufsize)
    print(data.decode('utf-8'))
except Exception as e:
    print(e)

client.close()