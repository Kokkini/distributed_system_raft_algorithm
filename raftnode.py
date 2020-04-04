import rpyc
import sys
import argparse
import random
import threading
import time

CHECK_PERIOD = 0.05



argv = argparse.ArgumentParser()
argv.add_argument("config")
argv.add_argument("node_id")
argv.add_argument("port")
argv = argv.parse_args()

def flush_print(*values):
    print(*values, flush=True)



'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''
class RaftNode(rpyc.Service):


    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """
    def __init__(self, config, id):
        self.voted = False
        self.heartbeat_period = 0.2
        self.check_period = 0.05
        flush_print(id)
        # flush_print(self.read_config(config))
        self.config = self.read_config(config)
        self.N = len(self.config)
        self.id = id
        # self.other_nodes = self.get_other_nodes(self.config, self.id)
        # self.other_nodes["1"].root.say_hi()
        self.send_heartbeat()
        self.is_leader = False
        self.elect_time = random.randint(100, 350) / 100 #second
        flush_print(f"elect time {self.elect_time}")
        self.last_heard = time.time()
        self.term = 0
        self.num_votes = 0
        threading.Thread(target=self.start_beating).start()
        threading.Thread(target=self.monitor_leader_beat).start()



    def heartbeat_send_thread(self, id, ip, port):
        try:
            c = rpyc.connect(ip, port)
            c.root.receive_heartbeat(self.id, self.is_leader, self.term)
        except OSError:
            # flush_print(f"node {id} is down")
            return

    def monitor_leader_beat(self):
        while True:
            if self.is_leader:
                self.last_heard = time.time()
                time.sleep(self.check_period)
            if time.time() - self.last_heard > self.elect_time:
                self.send_candidate_message()
            else:
                time.sleep(self.check_period)

    def exposed_vote(self, id, term):
        if self.term >= term:
            return
        self.term = term
        self.last_heard = time.time()
        ip, port = self.config[id]
        try:
            c = rpyc.connect(ip, port)
            c.root.receive_vote(self.id, self.term)
        except OSError:
            # flush_print(f"node {id} is down")
            return

    def exposed_receive_vote(self, id, term):
        if term == self.term:
            self.num_votes += 1
        if self.num_votes + 1 > self.N / 2:
            self.is_leader = True
            flush_print(f"node {self.id} is the leader with {self.num_votes+1} votes")


    def start_beating(self):
        while True:
            if self.is_leader:
                self.send_heartbeat()
                time.sleep(self.heartbeat_period)

    def send_heartbeat(self):
        for id in self.config:
            if id == self.id:
                continue
            ip, port = self.config[id]
            threading.Thread(target=self.heartbeat_send_thread, args=(id, ip, port, )).start()

    def candidate_message_thread(self, ip, port):
        try:
            c = rpyc.connect(ip, port)
            c.root.vote(self.id, self.term)
        except OSError:
            # flush_print(f"node {id} is down")
            return

    def send_candidate_message(self):
        self.last_heard = time.time()
        self.term += 1
        flush_print(f"became candidate for term {self.term}")
        self.num_votes = 0
        for id in self.config:
            if id == self.id:
                continue
            ip, port = self.config[id]
            threading.Thread(target=self.candidate_message_thread, args=(ip, port, )).start()

    def exposed_receive_heartbeat(self, other_id, is_leader, term):
        # flush_print(f"received heart beat from {other_id}")
        if is_leader:
            if self.term > term:
                return
            elif self.term < term:
                self.is_leader = False
                self.term = term
            self.last_heard = time.time()

    def get_other_nodes(self, config_dict, this_id):
        nodes = {}
        for id in config_dict:
            if id == this_id:
                continue
            ip, port = config_dict[id]
            nodes[id] = rpyc.connect(ip, id)



    '''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
    '''
    def exposed_is_leader(self):
        return self.leader_id == self.id

    def read_config(self, file):
        config_dict = {}
        with open(file) as f:
            N = int(f.readline().split()[-1])
            for i in range(N):
                line = f.readline()
                id, ip, port_num = line.split(":")
                id = id[4:]
                ip = ip.strip()
                port_num = int(port_num.strip())
                config_dict[id] = (ip, port_num)
        return config_dict

    def exposed_say_hi(self):
        # flush_print(f"hello my name is {self.id}")
        return

if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer
    print("flag 1", flush=True)
    server = ThreadPoolServer(RaftNode(argv.config, argv.node_id), port = int(argv.port))
    server.start()
    print("flag 2", flush=True)
