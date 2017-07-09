import zmq
import time
import random
from multiprocessing import Process

#
def worker_a():
    context = zmq.Context.instance()
    worker = context.socket(zmq.DEALER)
    worker.setsockopt(zmq.IDENTITY, b'A')
    worker.connect("tcp://localhost:5556")

    total = 0
    while True:
        print "worker A"
        # We receive one part, with the workload
        request = worker.recv()
        finished = request == b"END"
        if finished:
            print("A received: %s" % total)
            break
        total += 1

def worker_b():
    context = zmq.Context.instance()
    worker = context.socket(zmq.DEALER)
    worker.setsockopt(zmq.IDENTITY, b'B')
    worker.connect("tcp://localhost:5556")

    total = 0
    while True:
        print "worker B"
        # We receive one part, with the workload
        request = worker.recv()
        finished = request == b"END"
        if finished:
            print("B received: %s" % total)
            break
        total += 1

context = zmq.Context.instance()
client = context.socket(zmq.ROUTER)
client.bind("tcp://*:5556")

Process(target=worker_a, args='').start()
Process(target=worker_b, args='').start()
# Thread(target=worker_a).start()
# Thread(target=worker_b).start()

# Wait for threads to stabilize
time.sleep(1)

# Send 10 tasks scattered to A twice as often as B
for _ in range(10):
    # Send two message parts, first the address
    ident = random.choice([b'A', b'A', b'B'])
    # And then the workload
    work = b"This is the workload"
    client.send_multipart([ident, work])

client.send_multipart([b'A', b'END'])
client.send_multipart([b'B', b'END'])