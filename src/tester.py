import argparse
import xmlrpc.client


def updatefile(hostport):
    client = xmlrpc.client.ServerProxy('http://' + hostport)
    client.surfstore.ping()
    print("Ping() successful")
    print('[%s] leader: %s' % (hostport, client.surfstore.isLeader()))
    print('[%s] crashed: %s' % (hostport, client.surfstore.isCrashed()))
    print(client.surfstore.getfileinfomap())
    client.surfstore.updatefile("test.txt", 1, [1, 2, 3])
    print(client.surfstore.getfileinfomap())

def crash(hostport):
    client = xmlrpc.client.ServerProxy('http://' + hostport)
    client.surfstore.ping()
    print("Ping() successful")
    print('[%s] leader: %s' % (hostport, client.surfstore.isLeader()))
    print('[%s] crashed: %s' % (hostport, client.surfstore.isCrashed()))
    client.surfstore.crash()
    print('[%s] crashed: %s' % (hostport, client.surfstore.isCrashed()))

def restore(hostport):
    client = xmlrpc.client.ServerProxy('http://' + hostport)
    client.surfstore.ping()
    print("Ping() successful")
    print('[%s] leader: %s' % (hostport, client.surfstore.isLeader()))
    print('[%s] crashed: %s' % (hostport, client.surfstore.isCrashed()))
    client.surfstore.restore()
    print('[%s] crashed: %s' % (hostport, client.surfstore.isCrashed()))

if __name__ == "__main__":

    # parser = argparse.ArgumentParser(description="SurfStore client")
    # parser.add_argument('hostport', help='host:port of the server')
    # args = parser.parse_args()
    #
    # hostport = args.hostport
    # crash('localhost:10013')
    # crash('localhost:10012')
    updatefile('localhost:10015')
    # crash('localhost:10016')
    # crash('localhost:10015')
    # restore('localhost:10013')
    # restore('localhost:10012')
    # restore('localhost:10016')
    # restore('localhost:10015')

    # crash('localhost:10016')
    # crash('localhost:10014')
    # crash('localhost:10013')
    # crash('localhost:10015')

    # try:
    # hostport = 'localhost:10015'

    # hostport = 'localhost:10016'

        # client.surfstore.restore()
        # print('restored: %s' % hostport)
        # client.surfstore.updatefile("test.txt", 4, [1, 2, 3])
    # hostport='localhost:10012'
    # client = xmlrpc.client.ServerProxy('http://' + hostport)
    # print(client.surfstore.getfileinfomap())
        # client.surfstore.tester_getversion("Test.txt")

        # except Exception as e:
        # 	print("Client: " + str(e))
