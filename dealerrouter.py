import zmq
import time
import timeit as ti
import uuid
from threading import Thread
from optparse import OptionParser

###############################################################################
# This class is used to give switch statement like behavior from C/C++
# There's no magic here, other than making code elsewhere readable
class switch(object):
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        yield self.match
        raise StopIteration

    def match(self, *args):
        if self.fall or not args:
            return True
        elif self.value in args:
            self.fall = True
            return True
        else:
            return False
###############################################################################
class Proto():
    # this is just a namespace for storing protocol IDs
    # TODO: this is ghetto
    headerlen   = 6
    greet       =     b'0x0001'
    str         =     b'0x0002'
    serverstop  =     b'0x000A'
    clientstop  =     b'0x000B'
###############################################################################
class Client():
    # client registration string

    def __init__(self, server_port):
        context = zmq.Context().instance()
        self.socket = context.socket(zmq.DEALER)
        # generate a universally unique client ID
        self.id = uuid.uuid4()
        self.socket.setsockopt(zmq.IDENTITY, str(self.id))
        self.socket.connect("tcp://localhost:%s" % server_port)
        # set up a read poller
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        # send connection message that will register server with client
        self.send(Proto.greet)
        print "Client: " + str(self.id) + " connected to port: " + str(server_port)

    def run(self):
        print "Client: start"

        # Client's idle loop
        timeout = 1
        total = 0
        while True:
            # Read incoming
            sockets = dict(self.poller.poll(timeout))
            if self.socket in sockets and sockets[self.socket] == zmq.POLLIN:
                msg = self.socket.recv()
                total += 1
                if not self.parseMsg(msg):
                    break

            # Send outgoing
            work = b"workload" + str(total)
            self.send(Proto.str, work)

        print("Client: total messages received: %s" % total)
        print "Client: end"

    def send(self, proto, data = b''):
        self.socket.send(proto + data)

    def parseMsg(self, msg):
        ret = True
        header = msg[0:Proto.headerlen]
        body = msg[Proto.headerlen:]
        for case in switch(header):
            if case(Proto.str):
                print "Client: string: " + body
                break
            if case(Proto.serverstop):
                print "Client: serverstop"
                # Send reply to delete client
                self.send(Proto.clientstop)
                ret = False
                break
            if case():  # default
                print "Client: received undefined message!"
                # TODO: debug
        return ret
###############################################################################
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
        # set up a read poller
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.clientmap = dict()
        print "Server bound to port: " + str(server_port)

    def run(self, runtime):
        print "Server: start"

        # Server's idle loop
        # Note: artificially running for a fixed number of seconds
        timeout = 1 # int(1.0/60.0)
        loopstart = ti.default_timer()
        running = True
        while running:
            # Read incoming
            sockets = dict(self.poller.poll(timeout))
            if self.socket in sockets and sockets[self.socket] == zmq.POLLIN:
                id, data = self.socket.recv_multipart()
                self.parseMsg(id, data)

            # Send outgoing
            for x in range(10):
                for id in self.clientmap.iterkeys():
                    work = b"workload" + str(x)
                    self.send(id, Proto.str, work)

            loopstop = ti.default_timer()
            if loopstop - loopstart > runtime:
                running = False

        # Force disconnect/kill all clients
        print "Server: shutting down..."
        for id in self.clientmap.iterkeys():
            self.send(id, Proto.serverstop)

        print self.clientmap
        self.socket.close()
        print "Server: end"

    def send(self, id, proto, data = b''):
        final = proto + data
        self.socket.send_multipart([id, final])
        # update send stats
        usage = self.clientmap[id]
        usage['omc'] += 1
        usage['obs'] += len(final)

    def parseMsg(self, id, msg):
        header = msg[0:Proto.headerlen]
        body = msg[Proto.headerlen:]

        # Check if client is registered -- this is messy
        if not id in self.clientmap and not header == Proto.greet:
            print "Server: recv'd msg from unregistered client"
            # TODO: debug
            return

        for case in switch(header):
            if case(Proto.greet):
                self.addClient(id, body)
                break
            if case(Proto.str):
                print "Server: string: (" + id + ") " + body
                break
            if case(Proto.clientstop):
                self.removeClient(id, body)
                break
            if case():  # default
                print "Server: received undefined message!"
                # TODO: debug

        # update receive stats
        usage = self.clientmap[id]
        usage['imc'] += 1
        usage['ibr'] += (Proto.headerlen + len(body))

    def addClient(self, id, body):
        if id in self.clientmap:
            print "Server: recv'd duplicate client reg"
            # TODO: debug
        else:
            print "Server: registering new client"
            self.clientmap[id] = {'imc': 0, 'ibr': 0, 'omc': 0, 'obs': 0}
            print self.clientmap

    def removeClient(self, id, body):
        if id in self.clientmap:
            print "Server: removing client (" + id + ")"
            del self.clientmap[id]
        else:
            print "Attempt to remove unregistered client"
            # TODO: debug
###############################################################################
if __name__ == "__main__":
    # set up command line arguments using optparse library
    usage = "usage: %prog [options] arg1 arg2"
    parser = OptionParser(usage, version="%prog 0.1")
    parser.add_option("-s", "--server", action="store_true", dest="server",
                      default=False, help="enable server-only run mode")
    parser.add_option("-c", "--client", action="store_true", dest="client",
                      default=False, help="enable client-only run mode")
    parser.add_option("-r", "--run", type="int", dest="runtime", default=10,
                      help="set run time [10]")
    (options, args) = parser.parse_args()

    server_port = 5556
    # server mode takes precedence
    if (options.server):
        print "--server only mode--"
        # start the server
        server = Server(server_port)
        server.run(options.runtime)
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
        Thread(target=server.run, args='options.runtime').start()
###############################################################################
