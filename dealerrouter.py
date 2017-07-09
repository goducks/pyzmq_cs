import zmq
import time
from threading import Thread
from optparse import OptionParser

c_socket = None
def client(server_port):
    global c_socket
    context = zmq.Context().instance()
    c_socket = context.socket(zmq.DEALER)
    c_socket.setsockopt(zmq.IDENTITY, b'A')
    c_socket.connect("tcp://localhost:%s" % server_port)

def client_run():
    global c_socket
    total = 0
    print "client run start"
    while True:
        c_socket.send(b"ready")
        # We receive one part, with the workload
        request = c_socket.recv()
        print "client received: " + request
        finished = request == b"END"
        if finished:
            print("A received: %s" % total)
            break
        total += 1

    print "client run end"

server = None
def server(server_port):
    global server
    context = zmq.Context().instance()
    server = context.socket(zmq.ROUTER)
    server.bind("tcp://*:%s" % server_port)

def server_run():
    global server
    print "server run start"

    identity, bounce = server.recv_multipart()
    print "server received from: " + identity + ", message: " + bounce

    for _ in range(10):

        # Send two message parts, first the address
        ident = b'A'
        # And then the workload
        work = b"Workload"
        server.send_multipart([ident, work])

    server.send_multipart([b'A', b'END'])
    print "server run end"

if __name__ == "__main__":
    # set up command line arguments using optparse library
    usage = "usage: %prog [options] arg1 arg2"
    parser = OptionParser(usage, version="%prog 0.1")
    parser.add_option("-s", "--server", action="store_true", dest="server",
                      default=False, help="enable server-only run mode")
    parser.add_option("-c", "--client", action="store_true", dest="client",
                      default=False, help="enable client-only run mode")
    (options, args) = parser.parse_args()

    server_port = 5556
    # server mode takes precedence
    if (options.server):
        print "--server only mode--"
        # start the server
        server(server_port)
        server_run()
    elif (options.client):
        print "--client only mode--"
        # start the client
        client(server_port)
        client_run()
    else:
        print "--client and server mode--"
        # start both
        server(server_port)
        client(server_port)
        time.sleep(0.1)
        print "starting run threads"
        Thread(target=client_run, args='').start()
        Thread(target=server_run, args='').start()
