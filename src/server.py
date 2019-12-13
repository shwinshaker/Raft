from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
import argparse
from node import *


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


# A simple ping, returns true
def ping():
    """A simple ping method"""
    print("Ping()")
    return True


# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    print("GetBlock(" + h + ")")

    blockData = bytes(4)
    return blockData


# Puts a block
def putblock(b):
    """Puts a block"""
    print("PutBlock()")

    return True


# Given a list of hashes, return the subset that are on this server
def hasblocks(hashlist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")

    return hashlist


# Retrieves the server's FileInfoMap
def getfileinfomap():
    """Gets the fileinfo map"""
    # print("GetFileInfoMap()")
    if node.is_leader():
        while not node.majority_of_nodes_working():
            pass
        return node.file_info_map
    else:
        raise Exception('Not a leader!')


# Update a file's fileinfo entry
def updatefile(filename, version, hashlist):
    """Updates a file's fileinfo entry"""
    # print("UpdateFile("+filename+")")
    if node.is_leader():
        while not node.majority_of_nodes_working():
            pass
        entry = Entry(node.log[-1].index+1, node.term, filename, version, hashlist)
        node.add(entry)
        # entry.run(node.file_info_map)
        # node.file_info_map[filename] = [version, hashlist]
        return True
    else:
        raise Exception('Not a leader!')


# PROJECT 3 APIs below


# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    # print("IsLeader()")
    return node.is_leader()


# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    # print("Crash()")
    node.is_crashed = True
    node.clear_timer()
    return True


# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    # print("Restore()")
    node.is_crashed = False
    node.switch(node.state)
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed(request=None):
    """Returns whether this node is crashed or not"""
    # print("IsCrashed()")
    return node.is_crashed


# Requests vote from this server to become the leader
# def requestVote(serverid, term):
# def requestVote(*args, **kwargs):
#     return node.handle_vote(*args, **kwargs)
def requestVote(request):
    return node.handle_vote(request)

    # """Requests vote to be the leader"""
    # if node.is_crashed:
    #     raise Exception('isCrashed!')
    # if node.term < term:
    #     node.term = term
    #     node.switch('Follower')
    #     '''should be both ok'''
    #     return term, True
    #     # return node.term, True
    # return node.term, False

    # '''
    #     Why need a serverid here?
    #     voteFor should be null every time an election begins
    # '''
    # if node.voteFor is None or node.voteFor == serverid:
    #     '''
    #         should compare log too
    #     '''
    #     # raise NotImplementedError()
    #     node.voteFor = serverid
    #     return node.term, True
    #
    # # otherwise, should not vote
    # return node.term, False


# Updates fileinfomap
def appendEntries(*args, **kwargs):
    return node.handle_appendEntry(*args, **kwargs)

# def appendEntries(serverid, term, fileinfomap):
#     """Updates fileinfomap to match that of the leader"""
#     if node.is_crashed:
#         raise Exception('isCrashed!')
#
#     if node.term > term:
#         return node.term, False
#     else:
#         node.switch('Follower', term)
#         node.file_info_map = fileinfomap
#         '''should be both ok'''
#         return term, True


def tester_getversion(filename):
    return node.file_info_map[filename][0]


# Reads the config file and return host, port and store list of other servers
def readconfig(config, servernum):
    """Reads cofig file"""
    fd = open(config, 'r')
    l = fd.readline()

    maxnum = int(l.strip().split(' ')[1])

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()

    host, port = None, None

    # server list has list of other servers
    serverlist = []

    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        else:
            serverlist.append(hostport)

    return maxnum, host, port, serverlist


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="SurfStore server")
        parser.add_argument('config', help='path to config file')
        parser.add_argument('servernum', type=int, help='server number')

        args = parser.parse_args()

        config = args.config
        servernum = args.servernum

        # maxnum is maximum number of servers
        maxnum, host, port, serverlist = readconfig(config, servernum)

        node = Node(servernum, serverlist)

        print("Attempting to start XML-RPC Server...")
        print(host, port)
        server = ThreadedXMLRPCServer((host, port), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")
        # Project 3 APIs
        server.register_function(isLeader,"surfstore.isLeader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.isCrashed")
        server.register_function(requestVote,"surfstore.requestVote")
        server.register_function(appendEntries,"surfstore.appendEntries")
        server.register_function(tester_getversion,"surfstore.tester_getversion")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))
