#!./env python

import xmlrpc.client
import random
from functools import reduce
from multiprocessing import Manager, Process
from timer_utils import *


class AppendEntriesRequest:

    @classmethod
    def from_json(cls, json):
        return cls(**json)

    def __init__(self, term, serverid, prevLogIndex=0, prevLogTerm=0, commitIndex=0, entries=[]):
        self.term = term
        self.serverid = serverid

        self.prevLogIndex = prevLogIndex
        self.prevLogTerm = prevLogTerm
        self.commitIndex = commitIndex
        # self.entries = [Entry(**entry) if isinstance(entry, dict) else entry for entry in entries]
        self.entries = [Entry.from_json(entry) if isinstance(entry, dict) else entry for entry in entries]

    def __repr__(self):
        return '[term]{}: [id]{}: [prevIndex]{}: [prevTerm]{}: [commitIndex]{}: [entries]{}'.format(self.term, self.serverid, self.prevLogIndex, self.prevLogTerm, self.commitIndex, self.entries)


class AppendEntriesReply:

    @classmethod
    def from_json(cls, json):
        return cls(**json)

    def __init__(self, term, success, matchIndex=0):
        # matchIndex is related to log roll back
        # currently unavailable
        self.term = term
        self.success = success
        self.matchIndex = matchIndex


class RequestVoteRequest:

    @classmethod
    def from_json(cls, json):
        return cls(**json)

    def __init__(self, term, serverid, lastLogIndex=0, lastLogTerm=0):
        self.term = term
        self.serverid = serverid
        self.lastLogIndex = lastLogIndex
        self.lastLogTerm = lastLogTerm


class RequestVoteReply:

    @classmethod
    def from_json(cls, json):
        return cls(**json)

    def __init__(self, term, success):
        self.term = term
        self.success = success


class Entry:

    @classmethod
    def from_json(cls, json):
        return cls(**json)

    def __init__(self, index=0, term=0, filename='', version='', hashlist=[]):
        self.index = index
        self.term = term
        self.filename = filename
        self.version = version
        self.hashlist = hashlist

    def __eq__(self, other):
        return (self.index, self.term, self.filename, self.version, self.hashlist) == (other.index, other.term, other.filename, other.version, other.hashlist)

    def run(self, file_info_map):
        # file_info_map is a state machine
        file_info_map[self.filename] = [self.version, self.hashlist]

    def __repr__(self):
        return '[index]: {} [term]: {} [filename]: [] [version]: []'.format(self.index, self.term, self.filename, self.hashlist)


class Node:

    # __electionTimeout = (4, 5)  # 150 - 300 ms
    # __broadcastTimeout = 1  # 50 ms

    __electionTimeout = (0.5, 1)
    # __electionTimeout = (0.15, 0.3)  # 150 - 300 ms
    __broadcastTimeout = 0.05  # 50 ms
    __commitTimeout = 0.001

    __states = {'Follower', 'Leader', 'Candidate'}

    def __init__(self, serverid, serverlist):

        self.serverid = serverid
        self.serverlist = serverlist
        self.clients = dict()
        self.num_servers = len(serverlist) + 1

        self.state = None
        self.is_crashed = False

        self.term = 0
        self.voteFor = None
        self.log = [Entry()] # log # initial an empty entry to pass the consistency check at the very first
        # self.file_info_map = dict() # state machine
        self.file_info_map = {} # {'test.txt': [-1, []]}

        self.commitIndex = 0
        self.lastApplied = 0

        # only for leader
        self.nextIndex = []
        self.matchIndex = []

        self._append_entry = None
        self._request_vote = None
        self._election_timer = None

        # commit timer, whenever possible, commit changes
        self._commit = RepeatingTimer(self.term, self.__commitTimeout, self.commit)
        self.switch('Candidate')

    def add(self, entry):
        # ensure index consistency  # incase mixed appendentry
        entry.index = len(self.log)
        self.log.append(entry)
        print('add entry at %i' % entry.index)

    def commit(self):
        if not self.commitIndex > self.lastApplied:
            return

        print('last committed: %i; commit to %i' % (self.lastApplied, self.commitIndex))
        while not self.commitIndex <= self.lastApplied:
            self.lastApplied += 1
            self.log[self.lastApplied].run(self.file_info_map)

        print('last committed: %i' % self.lastApplied)

    def clear_timer(self):
        if self._append_entry and self._append_entry.is_alive():
            self._append_entry.cancel()

        if self._request_vote and self._request_vote.is_alive():
            self._request_vote.cancel()

        if self._election_timer and self._election_timer.is_alive():
            print('{} timer {}'.format(self.serverid, self._election_timer.interval))
            self._election_timer.cancel()

    def switch(self, state, term=None):
        assert state in self.__states, 'State {} invalid!'.format(state)
        print(self.serverid, 'switch from {} to {}'.format(self.state, state))
        self.clear_timer()
        self.state = state

        if self.is_crashed:
            return

        if state == 'Follower':
            if term:
                self.term = term
            self.voteFor = None
            self._election_timer = OnceTimer(self.term, self.__electionTimeout, self.switch, ['Candidate'])

        elif state == 'Leader':
            self.nextIndex = dict([(hostport, self.log[-1].index+1) for hostport in self.serverlist])
            self.matchIndex = dict([(hostport, 0) for hostport in self.serverlist])
            self._append_entry = RepeatingTimer(self.term, self.__broadcastTimeout, self.append_entry)

        elif state == 'Candidate':
            self._request_vote = RepeatingTimer(self.term, self.__electionTimeout, self.request_vote)
        else:
            raise KeyError(state)

    def is_leader(self):
        if self.is_crashed:
            return False
        else:
            return self.state == 'Leader'

    # def call(self, replies, host_port, func, args, kwargs):
    #     if host_port not in self.clients:
    #         self.clients[host_port] = xmlrpc.client.ServerProxy('http://{}'.format(host_port))
    #     try:
    #         replies.append(getattr(self.clients[host_port].surfstore, func)(*args, **kwargs))
    #     except Exception as e:
    #         print('[{}] Fail to call {} {}'.format(e, func, host_port))
    #         pass
    #
    # def call_all(self, func, *args, **kwargs):
    #     replies = Manager().list()
    #     workers = list()
    #     for host_port in self.serverlist:
    #         p = Process(target=self.call, args=(replies, host_port, func, args, kwargs))
    #         p.start()
    #         workers.append(p)
    #     for p in workers:
    #         p.join()
    #     return replies

    def call(self, replies, host_port, func, request):
        if host_port not in self.clients:
            self.clients[host_port] = xmlrpc.client.ServerProxy('http://{}'.format(host_port))
        try:
            # replies.append(getattr(self.clients[host_port].surfstore, func)(request))
            replies[host_port] = getattr(self.clients[host_port].surfstore, func)(request)
        except Exception as e:
            print('[{}] Fail to call {} {}'.format(e, func, host_port))
            pass

    def call_all(self, func, requests=[]):
        replies = Manager().dict()  # list()
        workers = list()
        if not requests:
            requests = [''] * len(self.serverlist)
        for host_port, request in zip(self.serverlist, requests):
            p = Process(target=self.call, args=(replies, host_port, func, request))
            p.start()
            workers.append(p)
        for p in workers:
            p.join()
        return replies

    def majority_of_nodes_working(self):
        num = self.num_servers - sum(list(self.call_all('isCrashed').values()))
        return num > self.num_servers / 2

    def marshal_appendEntry(self):
        requests = []
        for hostport in self.serverlist:
            request = AppendEntriesRequest(self.term, self.serverid)
            if self.log[-1].index >= self.nextIndex[hostport]:
                request.prevLogIndex = self.nextIndex[hostport] - 1
                request.prevLogTerm = self.log[request.prevLogIndex].term
                request.entries = self.log[self.nextIndex[hostport]:]
                request.commitIndex = self.commitIndex
            else:
                request.prevLogIndex = self.log[-1].index
                request.prevLogTerm = self.log[request.prevLogIndex].term
                request.entries = []
                request.commitIndex = self.commitIndex
            requests.append(request)
        return requests

    def marshal_vote(self):
        request = RequestVoteRequest(self.term, self.serverid)
        request.lastLogIndex = self.log[-1].index
        request.lastLogTerm = self.log[-1].term
        return [request] * len(self.serverlist)

    def handle_appendEntry(self, request):
        # crash check
        if self.is_crashed:
            raise Exception('Im crashed!')

        # instanize
        request = AppendEntriesRequest.from_json(request)
        print('[AppendEntriesRequest handle] %s ' % request)

        # term check
        if request.term < self.term:
            return AppendEntriesReply(self.term, False)

        if request.term > self.term:
            self.switch('Follower', request.term)
            '''
                not sure if should success here
            '''
            # return AppendEntriesReply(request.term, True)

        # log consistency check
        print('consistency check')
        if self.log[-1].index >= request.prevLogIndex and self.log[request.prevLogIndex].term != request.prevLogTerm:
            return AppendEntriesReply(self.term, False)

        # append entries to log
        print('appending entries')
        if request.entries:
            for entry in request.entries:
                print('entry')
                if self.log[-1].index >= entry.index and self.log[entry.index].term != entry.term:
                    print('entry roll back')
                    index = self.log[-1].index
                    while index >= entry.index:
                        self.log.pop()
                        index -= 1
                '''
                    not very sure what does 'already in' mean
                '''
                print('check entry in log')
                if entry not in self.log:
                    print('log appended')
                    self.add(entry)

        # change will be committed later some time
        # this must be done after entries are appended to log
        if request.commitIndex > self.commitIndex:
            if request.entries:
                self.commitIndex = min(request.commitIndex, request.entries[-1].index)
            else:
                self.commitIndex = request.commitIndex
            # self.commitIndex = min(request.commitIndex, self.log[-1].index)

        print('switch follower')
        self.switch('Follower', request.term)
        return AppendEntriesReply(self.term, True, request.prevLogIndex)

    def handle_vote(self, request):
        # crash check
        if self.is_crashed:
            raise Exception('Im crashed!')

        request = RequestVoteRequest.from_json(request)

        # term check
        if request.term < self.term:
            return RequestVoteReply(self.term, False)

        if request.term > self.term:
            self.switch('Follower', request.term)
            # return RequestVoteReply(request.term, True)

            if self.voteFor is None or self.voteFor == request.serverid:
                # at least as up-to-date as in terms of last log's term and index
                if (request.lastLogTerm > self.log[-1].term) or \
                   (request.lastLogTerm == self.log[-1].term and request.lastLogIndex >= self.log[-1].index):
                    self.voteFor = request.serverid
                    return RequestVoteReply(self.term, True)

        return RequestVoteReply(self.term, False)

    def check_marjority(self, replies):
        # num_vote = sum(list(map(lambda x: x.success, replies))) + 1
        num_vote = sum([replies[hostport].success for hostport in replies]) + 1
        if num_vote > self.num_servers / 2:
            return True
        return False

    def check_term(self, replies):
        max_term = max([replies[hostport].term for hostport in replies])
        if max_term > self.term and not self.is_crashed:
            self.switch('Follower', max_term)
            return False
        return True

    def check_nextIndex(self, replies):
        for hostport in replies:
            reply = replies[hostport]
            if reply.success:
                self.matchIndex[hostport] = reply.matchIndex # self.nextIndex[hostport]
                self.nextIndex[hostport] = self.log[-1].index + 1
            else:
                '''
                    will there be the case this nextindex keeps decrementing to negative infinity?
                    not possible.
                    appendentry will only return false when the follower's term is higher
                        at this point the leader will already transit to follower
                '''
                self.nextIndex[hostport] -= 1

    def append_entry(self):
        requests = self.marshal_appendEntry()
        for request in requests:
            print('[appendEntries marshal] %s' % request)
        print('[nextIndex:]', self.nextIndex)
        print('[matchIndex:]', self.matchIndex)
        print(self.log)
        replies = self.call_all('appendEntries', requests)
        print('Node {} Term {} heart beat {}'.format(self.serverid, self.term, replies))
        replies = dict([(hostport, AppendEntriesReply.from_json(replies[hostport])) for hostport in replies])
        # replies = [ if replies[hostport] for hostport in self.serverlist] # instanize
        if not replies: return
        if not self.check_term(replies): return
        self.check_nextIndex(replies)
        if self.check_marjority(replies):
            '''
                simple version, didn't consider roll back
                not sure about the relation between commitIndex and lastApplied here
            '''
            for n in range(self.log[-1].index, self.commitIndex, -1):
                if sum([self.matchIndex[hostport] >= n for hostport in self.serverlist])+1 > self.num_servers/2 and \
                        self.log[n].term == self.term:
                    self.commitIndex = n
                    break

    def request_vote(self):
        self.term += 1
        self.voteFor = self.serverid

        requests = self.marshal_vote()
        replies = self.call_all('requestVote', requests)
        print('Node {} Term {} request vote {}'.format(self.serverid, self.term, replies))
        # replies = [RequestVoteReply.from_json(reply) for reply in replies] # instanize
        replies = dict([(hostport, RequestVoteReply.from_json(replies[hostport])) for hostport in replies])
        if not replies or self.state != 'Candidate': return
        if not self.check_term(replies): return

        if self.check_marjority(replies):
            self.switch('Leader')



