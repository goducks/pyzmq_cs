import zmq
import time
import uuid
from threading import Thread
from optparse import OptionParser

class Proto():
    # this is just a namespace for storing protocol IDs
    # TODO: this is ghetto
    greet =     str(0x00000001)
    kill =      str(0x0000000A)

class Client():
    # client registration string

    def __init__(self, server_port):
        context = zmq.Context().instance()
        self.socket = context.socket(zmq.DEALER)
        # generate a universally unique client ID
        self.id = uuid.uuid4()
        self.socket.setsockopt(zmq.IDENTITY, str(self.id))
        self.socket.connect("tcp://localhost:%s" % server_port)
        # send connection message that will register server with client
        self.socket.send(Proto.greet)
        print "Client: " + str(self.id) + " connected to port: " + str(server_port)

    def run(self):
        total = 0
        print "Client: start"
        while True:
            # We receive one part, with the workload
            request = self.socket.recv()
            print "Client: received " + request
            finished = request == Proto.kill
            if finished:
                print("Client: total messages received: %s" % total)
                break
            total += 1
        print "Client: end"

class Server():
    # To track clients, use dictionary of connections where:
    # key = client assigned identity
    # value = 4 element nested dict of:
    #    imc = incoming message count
    #    ibr = incoming bytes recv'd,
    #    omc = outgoing message count
    #    obs = outgoing bytes sent

    def __init__(self, server_port):
        context = zmq.Context().instance()
        self.socket = context.socket(zmq.ROUTER)
        self.socket.bind("tcp://*:%s" % server_port)
        self.clientmap = dict()
        print "Server bound to port: " + str(server_port)

    def run(self):
        print "Server: start"

        id, data = self.socket.recv_multipart()
        # Check for new clients first
        if not id in self.clientmap:
            if not data == Proto.greet:
                print "Server: recv'd message from unregistered client"
                # TODO: debug
            else:
                print "Server: registering new client"
                self.clientmap[id] = { 'imc' : 1, 'ibr' : len(Proto.greet), 'omc': 0, 'obs' : 0 }
                print self.clientmap
        elif data == Proto.greet:
            print "Server: recv'd duplicate registered client"
            # TODO: debug
        else:
            print "Server: handle message normally"
            self.parsemsg(id, data)

        for _ in range(10):
            for id in self.clientmap.iterkeys():
                # Send two message parts, first the address
                # And then the workload
                work = b"Workload"
                self.send(id, work)

        for id in self.clientmap.iterkeys():
            self.send(id, Proto.kill)

        print "Server: client stats"
        print self.clientmap
        print "Server: end"

    def send(self, id, data):
        self.socket.send_multipart([id, data])
        usage = self.clientmap[id]
        usage['omc'] += 1
        usage['obs'] += len(data)

    def parsemsg(self, id, data):
        pass

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
