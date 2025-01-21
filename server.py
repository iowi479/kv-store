import socket
import threading
from threading import Timer
import select
import os
import json
import sys
import time

ALL = 3
INFO = 2
ERROR = 1
NONE = 0

LOGGING_LEVEL = ALL
BROADCAST_PORT = 5000
BROADCAST_IP = "192.168.0.255"
MY_IP = os.environ['MY_IP']
MY_PORT = int(os.environ['MY_PORT'])

HEARTBEAT_INTERVAL = 10
HEARTBEAT_TIMEOUT = 3
JOIN_TIMEOUT = 1


class Server:
    def log(self, level, *messages):
        if LOGGING_LEVEL >= level:
            print("[" + self.pid + "]", *messages)

    def __init__(self):
        self.listening = True
        self.pid = MY_IP + ":" + str(MY_PORT)
        self.in_election = False
        self.ring = []
        self.neighbour_addr = None
        self.leader = None
        self.kv_cache = {}
        self.clients = []

        self.heartbeats = {}

        self.log(INFO, "Starting server")

        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.broadcast_socket.bind(('', BROADCAST_PORT))

        self.ring_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ring_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ring_socket.bind(('', MY_PORT))

        threading.Thread(target=self.listen).start()
        threading.Thread(target=self.monitor_heartbeats).start()

        self.send_join();

    def addr_from_pid(self, pid) -> tuple[str, int]:
        ip, port = pid.split(":")
        return (ip, int(port))

    def heartbeat_timout(self, pid):
        self.log(ERROR, "Heartbeat timeout for", pid)
        self.ring.remove(pid)
        self.heartbeats.pop(pid)
        self.broadcast_socket.sendto(str.encode("UPDATE: " + json.dumps(self.ring)), (BROADCAST_IP, BROADCAST_PORT))

        # TODO: maybe this doesnt work here
        # self.start_election()

    def clear_heartbeats(self):
        """This clears old hbs for left pids"""
        for pid in list(self.heartbeats):
            if pid not in self.ring:
                self.heartbeats.pop(pid)

    def monitor_heartbeats(self):
        while self.listening:
            time.sleep(1)
            for pid in self.ring:
                if pid == self.pid:
                    continue

                elif pid not in self.heartbeats:
                    self.heartbeats[pid] =  {'last_hb': 0, 'sent_ping': time.time()} 
                    self.ring_socket.sendto(str.encode("PING: " + json.dumps({'sender': self.pid})), self.addr_from_pid(pid))

                elif not self.heartbeats[pid]['sent_ping']:
                    if time.time() - self.heartbeats[pid]['last_hb'] > HEARTBEAT_INTERVAL:
                        self.heartbeats[pid]['sent_ping'] = time.time()
                        self.ring_socket.sendto(str.encode("PING: " + json.dumps({'sender': self.pid})), self.addr_from_pid(pid))

                else:
                    # we got a 'sent_ping'
                    if time.time() - self.heartbeats[pid]['sent_ping'] > HEARTBEAT_TIMEOUT:
                        self.heartbeat_timout(pid)
                        continue

    def listen(self):
        self.log(INFO, "Listening for messages now...")

        while self.listening:
            # This Line did it... A tribute for 2h of debugging.
            ready, _, _ = select.select([self.ring_socket, self.broadcast_socket], [], [])
            for s in ready:
                try:
                    data, addr = s.recvfrom(1024)
                except Exception as e:
                    self.log(ERROR, "Error while receiving data", e)
                    break

                if data.index(b": ") == -1:
                    self.log(ERROR, "Received invalid message", data)
                    continue

                [m_type, message] = data.decode().split(": ", 1)

                if m_type == "ACCEPT":
                    self.log(ALL, "Received ACCEPT message with ring: ", message)
                    ring = json.loads(message)
                    self.ring = ring
                    nb_pid = ring[ring.index(self.pid) - 1]
                    self.neighbour_addr = self.addr_from_pid(nb_pid)

                    self.start_election()

                elif m_type == "JOIN":
                    self.log(ALL, "Received JOIN message", message)
                    if self.leader == self.pid:
                        self.ring.append(message)
                        self.ring.sort()

                        self.broadcast_socket.sendto(str.encode("ACCEPT: " + json.dumps(self.ring)), (BROADCAST_IP, BROADCAST_PORT))


                elif m_type == "LEAVE":
                    self.log(ALL, "Received LEAVE message", message)
                    self.ring.remove(message)
                    # distribute that new ring
                    self.broadcast_socket.sendto(str.encode("UPDATE: " + json.dumps(self.ring)), (BROADCAST_IP, BROADCAST_PORT))


                elif m_type == "PING":
                    self.log(ALL, "Received PING message", message)
                    payload = json.loads(message)
                    payload['responder'] = self.pid
                    self.broadcast_socket.sendto(str.encode("PING_RESPONSE: " + json.dumps(payload)), addr)

                elif m_type == "PING_RESPONSE":
                    self.log(ALL, "Received PING_RESPONSE message", message)
                    payload = json.loads(message)

                    assert self.pid == payload['sender']
                    responder = payload['responder']

                    self.heartbeats[responder]['last_hb'] = time.time()
                    self.heartbeats[responder]['sent_ping'] = None


                elif m_type == "UPDATE":
                    self.log(ALL, "Received UPDATE message with ring: ", message)
                    ring = json.loads(message)
                    self.ring = ring
                    self.clear_heartbeats()
                    self.log(ALL, "New ring is", ring)
                    nb_pid = ring[ring.index(self.pid) - 1]
                    self.neighbour_addr = self.addr_from_pid(nb_pid)

                    if self.leader not in ring:
                        # we lost our leader -> election
                        self.start_election()


                elif m_type == "ELECTION":
                    self.log(ALL, "Received ELECTION message", message)
                    self.handle_election(json.loads(message))

                    # TODO: I think this doesnt work. Handle_election is only handeling a single message. With this, we replicate after every message in the election process
                    #for addr in self.ring:
                        #self.ring_socket.sendto(str.encode("REPLICATE: ", json.dumps(self.kv_cache)), addr)

                elif m_type == "STORE":
                    if self.leader == self.pid:
                        self.log(INFO, "Received STORE message", message)
                        data = json.loads(message)
                        if not data.get("key"):
                            self.log(ERROR, "Invalid key 'None'", message)
                        else:
                            self.kv_cache[data.get("key")] = data.get("value")
                            for addr in self.ring:
                                self.ring_socket.sendto(message, addr)
                    else:
                        self.log(ALL, "Ignoring STORE message because I'm not the leader...", message)


                elif m_type == "RETRIEVE":
                    if self.leader == self.pid:
                        self.log(INFO, "Received RETRIEVE message", message)
                        self.broadcast_socket.sendto(self.kv_cache.get(message), addr)
                    else:
                        self.log(ALL, "Ignoring RETRIEVE message because I'm not the leader...", message)


                elif m_type == "REPLICATE":
                    if self.leader == self.pid:
                        self.log(ERROR, "Received REPLICATE message but I'm leader", message)
                    else:
                        self.log(INFO, "Replicating data from leader", message)
                        data = json.loads(message)
                        self.kv_cache = data

                else:
                    self.log(ERROR, "Received unknown message", m_type, message)

    def send_join(self):
        self.broadcast_socket.sendto(str.encode("JOIN: " + self.pid), (BROADCAST_IP, BROADCAST_PORT))

        # TODO: check timeout to create ring with just us if nothing exists
        # should work like this
        threading.Timer(JOIN_TIMEOUT, self.create_ring).start()


    def election_message(self, mid, isLeader):
        election_body = {
                "mid": mid,
                "isLeader": isLeader
        } 
        return "ELECTION: " + json.dumps(election_body)

    def create_ring(self):
        """gets called if the initial JOIN message timed out which means there is no existing ring and we should create the initial one"""
        if not self.ring:
            self.ring = [self.pid]
            self.leader = self.pid
            self.log(INFO, "Creating initial ring with just only us since our JOIN message timed out after", JOIN_TIMEOUT, "second(s)")

            self.log(INFO, "Also creating a empty cache since Im the only one")
            self.kv_cache = {}

    def start_election(self):
        self.in_election = True

        self.log(INFO, "Starting election")

        if self.neighbour_addr:
            election_message = self.election_message(self.pid, False)
            self.log(ALL, "Sending election message to neighbour", self.neighbour_addr)
            self.log(ALL, "Election message:", election_message)
            self.ring_socket.sendto(election_message.encode(), self.neighbour_addr)

    def handle_election(self, election_body):
        if election_body['isLeader']:
            if not self.in_election:
                return

            self.leader = election_body['mid']
            self.in_election =  False

            self.log(INFO, "new leader is", self.leader)

            if self.neighbour_addr:
                election_message = self.election_message(self.leader, True)
                self.ring_socket.sendto(election_message.encode(), self.neighbour_addr)

            return

        if election_body['mid'] < self.pid and not self.in_election:
            self.in_election = True

            self.log(ALL, "Proposing myself as leader")

            if self.neighbour_addr:
                election_message = self.election_message(self.pid, False)
                self.ring_socket.sendto(election_message.encode(), self.neighbour_addr)

        elif election_body['mid'] > self.pid:
            self.in_election = True

            self.log(ALL, "Forwarding election message")

            if self.neighbour_addr:
                election_message = self.election_message(election_body['mid'], False)
                self.ring_socket.sendto(election_message.encode(), self.neighbour_addr)

        elif election_body['mid'] == self.pid:
            self.leader = self.pid
            self.in_election = False

            self.log(INFO, "I am the new leader and forwarding election message")

            if self.neighbour_addr:
                election_message = self.election_message(self.pid, True)
                self.ring_socket.sendto(election_message.encode(), self.neighbour_addr)

    def close(self):
        """greacefully close the server with a LEAVE message to the neighbour"""

        self.log(INFO, "Closing server")
        self.listening = False
        if self.neighbour_addr:
            self.log(INFO, "sending a LEAVE messge to my neighbour")
            self.ring_socket.sendto(str.encode("LEAVE: " + self.pid), self.neighbour_addr)
        else:
            self.log(INFO, "I have no neighbour to send a LEAVE message to")

        self.broadcast_socket.close()
        self.ring_socket.close()



def check_input():
    print("[CONFIG] Help: \n\t'q' to [q]uit\n\t'c' to [c]rash\n\t'v' to toggle [v]erbosity\n\n")
    while True:
        text = input()
        if text == "q":
            print("[CONFIG] [q]uitting")
            server.close()
            sys.exit(0)

        elif text == "c":
            print("[CONFIG] [c]rashing")
            sys.exit(1)

        elif text.startswith('v'):
            if len(text) == 2 and text[1].isdigit() and 0 <= int(text[1]) <= 3:
                level = int(text[1])
                global LOGGING_LEVEL
                LOGGING_LEVEL = level
                print("[CONFIG] set verbosity to:", level)
            else:
                print("[CONFIG] invalid verbosity level")




if __name__ == '__main__':
    threading.Thread(target=check_input).start()
    server = Server()
