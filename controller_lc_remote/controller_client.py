from socket import *
import argparse

parser = argparse.ArgumentParser(description='P4ML User Client')
parser.add_argument('--model', type=str, default='vgg16',
                    help='model to train')
parser.add_argument('--dataset', type=str, default='benchmark',
                    help='dataset to train')
parser.add_argument('--gpus', type=int, default=1,
                    help='number of training gpus')
parser.add_argument('--num_iters', type=int, default=10,
                    help='number of training iterations')
parser.add_argument('--is_MDJ', type=int, default=0,
                    help='is Meet Deadline Job')
parser.add_argument('--workers',nargs = '+', type=int,
                    help='which workers are used : []')
parser.add_argument('--ps', nargs = '+', type=int,
                    help='which server is used')

args = parser.parse_args()

host = '10.0.0.8'
port = 8888
bufsize = 1024
addr = (host, port)
client = socket(AF_INET, SOCK_STREAM)
client.connect((host, port))
print("client up, enter your job")


try:
    cmd = "control_" + str(args.gpus) + "_" + args.dataset + "_" + args.model + "_" + str(args.num_iters)
    cmd += "_" + str(args.is_MDJ) + "_" + str(args.workers) + "_" + str(args.ps)
    client.send(cmd.encode('utf-8'))
    print("submit job successfully!")
    print("waiting for job finishing...")
    data = client.recv(bufsize)
    print(data.decode('utf-8'))
except Exception as e:
    print(e)

client.close()