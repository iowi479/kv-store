import socket
import threading
import select
import os
import json
import sys

ALL = 3
INFO = 2
ERROR = 1
NONE = 0

LOGGING_LEVEL = INFO
BROADCAST_PORT = 5000
BROADCAST_IP = "192.168.0.255"
MY_IP = os.environ['MY_IP']
MY_PORT = int(os.environ['MY_PORT'])





class Server:
    def log(self, level, *messages):
        if LOGGING_LEVEL >= level:
            print("[" + self.pid + "]", *messages)

    def __init__(self):
        self.JOIN_TIMEOUT = 1
        self.pid = MY_IP + ":" + str(MY_PORT)
        self.in_election = False
        self.ring = []
        self.neighbour_addr = None
        self.leader = None
        self.kv_cache = {}
        self.clients = []

        self.log(INFO, "Starting server")

        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.broadcast_socket.bind(('', BROADCAST_PORT))

        self.ring_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ring_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ring_socket.bind(('', MY_PORT))

        threading.Thread(target=self.listen).start()
        self.send_join();

    def addr_from_pid(self, pid) -> tuple[str, int]:
        ip, port = pid.split(":")
        return (ip, int(port))

    def listen(self):
        self.log(INFO, "Listening for messages now...")

        while True:
            # This Line did it... A tribute for 2h of debugging.
            ready, _, _ = select.select([self.ring_socket, self.broadcast_socket], [], [])
            for s in ready:
                data, addr = s.recvfrom(1024)

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
                    # TODO: handle leave message
                    self.log(ERROR, "TODO: handle leave message")

                elif m_type == "ELECTION":
                    self.log(ALL, "Received ELECTION message", message)
                    self.handle_election(json.loads(message))
                    for addr in self.ring:
                        self.ring_socket.sendto(str.encode("REPLICATE: ", json.dumps(self.kv_cache)), addr)

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
        threading.Timer(self.JOIN_TIMEOUT, self.create_ring).start()


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
            self.log(INFO, "Creating initial ring with just only us since our JOIN message timed out after", self.JOIN_TIMEOUT, "second(s)")

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
