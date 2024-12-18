import socket
import threading
import select
import os
import json

BROADCAST_PORT = 5000
BROADCAST_IP = "192.168.0.255"
MY_IP = os.environ['MY_IP']
MY_PORT = int(os.environ['MY_PORT'])


class Server:
    def __init__(self):
        self.pid = MY_IP + ":" + str(MY_PORT)


        self.in_election = False
        self.ring = []
        self.neighbour_addr = None
        self.leader = None

        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.broadcast_socket.bind(('', BROADCAST_PORT))

        self.ring_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ring_socket.bind((MY_IP, MY_PORT))

        threading.Thread(target=self.listen).start()
        self.send_join();

    def listen(self):
        ready, _, _ = select.select([self.broadcast_socket, self.ring_socket], [], [])

        while True:
            for s in ready:
                data, addr = s.recvfrom(1024)
                [m_type, message] = data.decode().split(": ")
                print("Received broadcast message:", message)

                if m_type == "ACCEPT":
                    print("Received ACCEPT with ring:", message)
                    ring = json.loads(message)
                    self.ring = ring
                    self.neighbour_addr = ring[ring.index(self.pid) - 1]

                elif m_type == "JOIN":
                    print("Received JOIN message", message)
                    if self.leader == self.pid:
                        self.ring.append(message)
                        self.ring.sort()

                        self.broadcast_socket.sendto(str.encode("ACCEPT: " + json.dumps(self.ring)), (BROADCAST_IP, BROADCAST_PORT))

                elif m_type == "LEAVE":
                    print("Received LEAVE message", message)
                    # TODO:

                elif m_type == "ELECTION":
                    print("Received ELECTION message", message)
                    self.handle_election(json.loads(message))

    def send_join(self):
        self.broadcast_socket.sendto(str.encode("JOIN: " + self.pid), (BROADCAST_IP, BROADCAST_PORT))

        # TODO: check timeout to create ring with just us if nothing exists
        threading.Timer(5, self.create_ring).start()

    def create_ring(self):
        """gets called if the initial JOIN message timed out which means there is no existing ring and we should create the initial one"""
        self.ring = [self.pid]
        self.leader = self.pid

    def handle_election(self, election_message):
        if election_message['isLeader']:
            self.leader = election_message['mid']
            # forward received election message to left neighbour
            self.in_election =  False
            if self.neighbour_addr:
                self.ring_socket.sendto(json.dumps(election_message).encode(), self.neighbour_addr)

        if election_message['mid'] < self.pid and not self.in_election:
            new_election_message = {
                "mid": self.pid,
                "isLeader ": False
            }

            self.in_election = True
            # send received election message to left neighbour
            if self.neighbour_addr:
                self.ring_socket.sendto(json.dumps(new_election_message).encode(), self.neighbour_addr)

        elif election_message['mid'] > self.pid:
            # send received election message to left neighbour
            self.in_election = True
            if self.neighbour_addr:
                self.ring_socket.sendto(json.dumps(election_message).encode(), self.neighbour_addr)

        elif election_message['mid'] == self.pid:
            self.leader = self.pid
            new_election_message = {
                "mid": self.pid,
                "isLeader ": True
            }

            # send new election message to left neighbour
            self.in_election = False
            if self.neighbour_addr:
                self.ring_socket.sendto(json.dumps(new_election_message).encode(), self.neighbour_addr)

    def close(self):
        self.broadcast_socket.close()
        self.ring_socket.close()


if __name__ == '__main__':
    server = Server()
