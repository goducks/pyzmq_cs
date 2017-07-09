import zmq
import time
from threading import Thread
from optparse import OptionParser

class Client():
    def __init__(self, server_port):
        context = zmq.Context().instance()
        self.socket = context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, b'A')
        self.socket.connect("tcp://localhost:%s" % server_port)
        print "Client: " + b'A' + " connected to port: " + str(server_port)

    def run(self):
        total = 0
        print "Client run start"
        while True:
            self.socket.send(b"ready")
            # We receive one part, with the workload
            request = self.socket.recv()
            print "client received: " + request
            finished = request == b"END"
            if finished:
                print("A received: %s" % total)
                break
            total += 1
        print "client run end"

class Server():
    def __init__(self, server_port):
        context = zmq.Context().instance()
        self.socket = context.socket(zmq.ROUTER)
        self.socket.bind("tcp://*:%s" % server_port)
        print "Server bound to port: " + str(server_port)

    def run(self):
        print "Server run start"
        identity, bounce = self.socket.recv_multipart()
        print "server received from: " + identity + ", message: " + bounce

        for _ in range(10):
            # Send two message parts, first the address
            ident = b'A'
            # And then the workload
            work = b"Workload"
            self.socket.send_multipart([ident, work])

        self.socket.send_multipart([b'A', b'END'])
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
        server = Server(server_port)
        server.run()
    elif (options.client):
        print "--client only mode--"
        # start the client
        client = Client(server_port)
        client.run()
    else:
        print "--client and server mode--"
        # start both
        server = Server(server_port)
        client = Client(server_port)
        time.sleep(0.1)
        print "starting run threads"
        Thread(target=client.run, args='').start()
        Thread(target=server.run, args='').start()
