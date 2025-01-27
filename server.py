import socket, threading, select, json, time
from utils import addr_from_pid, check_input, ThreadSafeKVCache
from conf import (
    BROADCAST_PORT,
    BROADCAST_IP,
    MY_IP,
    JOIN_TIMEOUT,
    HEARTBEAT_INTERVAL,
    HEARTBEAT_TIMEOUT,
    ALL,
    INFO,
    ERROR,
    LOGGING_LEVEL,
)


class Server:
    def log(self, level, *messages):
        if LOGGING_LEVEL >= level:
            print("[" + self.pid + "]", *messages)

    def __init__(self):
        self.running = True

        self.in_election = False
        self.ring = []
        self.neighbour_addr = None
        self.leader = None
        self.kv_cache = ThreadSafeKVCache()
        self.clients = []

        self.heartbeats = {}

        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.broadcast_socket.bind(("", BROADCAST_PORT))

        self.ring_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ring_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ring_socket.bind(("", 0))
        self.pid = MY_IP + ":" + str(self.ring_socket.getsockname()[1])

        self.log(INFO, "Started server")

        self.client_socket = None

        self.rep = None
        self.replication = []

    def start(self):
        threading.Thread(target=self.listen).start()
        threading.Thread(target=self.monitor_heartbeats).start()

        self.send_join()

    def set_leader(self, leader):
        if leader == self.pid:
            self.leader = leader
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.client_socket.bind(("", 0))

            self.client_socket_addr = (
                MY_IP + ":" + str(self.client_socket.getsockname()[1])
            )

            self.listening_for_clients = True
            threading.Thread(target=self.listen_for_clients).start()

            return

        if self.leader == self.pid:
            self.log(INFO, "Im not leader anymore, closing client socket")
            self.listening_for_clients = False

            if self.client_socket:
                self.log(INFO, "Closing client socket")
                for conn in self.clients:
                    conn.shutdown(socket.SHUT_RDWR)
                    conn.close()

                try:
                    self.client_socket.shutdown(socket.SHUT_RDWR)
                except socket.error as e:
                    print("Socket not connected or already closed:", e)
                finally:
                    self.client_socket.close()

            self.client_socket = None

        self.leader = leader

    def listen_for_clients(self):
        while self.listening_for_clients and self.client_socket and self.running:
            try:
                self.client_socket.listen(1)
                conn, addr = self.client_socket.accept()
                self.clients.append(conn)

                threading.Thread(target=self.handle_client, args=[conn, addr]).start()
            except Exception as e:
                self.log(ERROR, "Error while accepting client", e)
                continue

    def handle_client(self, conn, addr):
        self.log(INFO, "New client connected", addr)

        try:
            while self.running:
                data = conn.recv(1024)
                if not data:
                    break

                if data.index(b": ") == -1:
                    self.log(ERROR, "Received invalid message", data)
                    continue

                [m_type, message] = data.decode().split(": ", 1)

                if m_type == "STORE":
                    self.log(INFO, "Received STORE message", message)

                    data = json.loads(message)

                    while self.rep:
                        time.sleep(1)

                    self.rep = time.time()

                    self.kv_cache.set(data["key"], data["value"])
                    self.replicate_data()

                    conn.send(str.encode("OK: " + json.dumps(data)))

                elif m_type == "RETRIEVE":
                    self.log(INFO, "Received RETRIEVE message", message)

                    data = json.loads(message)

                    value = self.kv_cache.get(data["key"])

                    data["value"] = value

                    conn.send(str.encode("DATA: " + json.dumps(data)))

                else:
                    self.log(ERROR, "Received unknown message", m_type, message)

        except ConnectionResetError as e:
            self.log(ERROR, "Connection reset by client", addr)
            conn.close()
            self.clients.remove(conn)

    def replicate_data(self):
        """this replicates the own data to all servers in the ring"""
        assert self.leader == self.pid

        self.rep = time.time()
        self.replication = self.ring.copy()

        # we remove ourself from the replication
        # since we already have the data
        self.replication.remove(self.pid)

        for addr in self.replication:
            if addr == self.pid:
                continue

            self.ring_socket.sendto(
                str.encode("REPLICATE: " + json.dumps(self.kv_cache.get_dict())),
                addr_from_pid(addr),
            )

        while len(self.replication) > 0:
            time.sleep(1)
            self.log(INFO, "Waiting for replication to finish")

        # replication done
        self.rep = None

    def heartbeat_timout(self, pid):
        self.log(ERROR, "Heartbeat timeout for", pid)
        self.ring.remove(pid)
        self.heartbeats.pop(pid)
        self.broadcast_socket.sendto(
            str.encode("UPDATE: " + json.dumps(self.ring)),
            (BROADCAST_IP, BROADCAST_PORT),
        )

    def clear_heartbeats(self):
        """This clears old hbs for left pids"""
        for pid in list(self.heartbeats):
            if pid not in self.ring:
                self.heartbeats.pop(pid)

    def monitor_heartbeats(self):
        while self.running:
            time.sleep(1)
            for pid in self.ring:
                if pid == self.pid:
                    continue

                elif pid not in self.heartbeats:
                    self.heartbeats[pid] = {"last_hb": 0, "sent_ping": time.time()}
                    self.ring_socket.sendto(
                        str.encode("PING: " + json.dumps({"sender": self.pid})),
                        addr_from_pid(pid),
                    )

                elif not self.heartbeats[pid]["sent_ping"]:
                    if (
                        time.time() - self.heartbeats[pid]["last_hb"]
                        > HEARTBEAT_INTERVAL
                    ):
                        self.heartbeats[pid]["sent_ping"] = time.time()
                        self.ring_socket.sendto(
                            str.encode("PING: " + json.dumps({"sender": self.pid})),
                            addr_from_pid(pid),
                        )

                else:
                    # we got a 'sent_ping'
                    if (
                        time.time() - self.heartbeats[pid]["sent_ping"]
                        > HEARTBEAT_TIMEOUT
                    ):
                        self.heartbeat_timout(pid)
                        continue

    def listen(self):
        self.log(INFO, "Listening for messages now...")

        while self.running:
            ready, _, _ = select.select(
                [self.ring_socket, self.broadcast_socket], [], []
            )

            for s in ready:
                try:
                    data, addr = s.recvfrom(1024)
                except Exception as e:
                    self.log(ERROR, "Error while receiving data", e)
                    break

                if data.index(b": ") == -1:
                    self.log(ERROR, "Received invalid message", data)
                    continue

                self.log(ALL, "Received message", data)
                [m_type, message] = data.decode().split(": ", 1)

                if m_type == "ACCEPT":
                    self.log(ALL, "Received ACCEPT message with ring: ", message)
                    ring = json.loads(message)
                    self.ring = ring
                    nb_pid = ring[ring.index(self.pid) - 1]
                    self.neighbour_addr = addr_from_pid(nb_pid)

                    self.start_election()

                elif m_type == "JOIN":
                    self.log(ALL, "Received JOIN message", message)
                    if self.leader == self.pid:
                        self.ring.append(message)
                        no_duplicates = list(set(self.ring))
                        self.ring = no_duplicates
                        self.ring.sort()

                        threading.Thread(target=self.replicate_data).start()
                        self.broadcast_socket.sendto(
                            str.encode("ACCEPT: " + json.dumps(self.ring)),
                            (BROADCAST_IP, BROADCAST_PORT),
                        )

                elif m_type == "LEAVE":
                    self.log(ALL, "Received LEAVE message", message)
                    self.ring.remove(message)

                    # if the pid was in a replication remove him as well
                    if message in self.replication:
                        self.replication.remove(message)

                    # distribute that new ring
                    self.broadcast_socket.sendto(
                        str.encode("UPDATE: " + json.dumps(self.ring)),
                        (BROADCAST_IP, BROADCAST_PORT),
                    )

                elif m_type == "PING":
                    self.log(ALL, "Received PING message", message)
                    payload = json.loads(message)
                    payload["responder"] = self.pid
                    self.broadcast_socket.sendto(
                        str.encode("PING_RESPONSE: " + json.dumps(payload)), addr
                    )

                elif m_type == "PING_RESPONSE":
                    self.log(ALL, "Received PING_RESPONSE message", message)
                    payload = json.loads(message)

                    assert self.pid == payload["sender"]
                    responder = payload["responder"]

                    self.heartbeats[responder]["last_hb"] = time.time()
                    self.heartbeats[responder]["sent_ping"] = None

                elif m_type == "UPDATE":
                    self.log(ALL, "Received UPDATE message with ring: ", message)
                    ring = json.loads(message)
                    self.ring = ring
                    self.clear_heartbeats()
                    self.log(ALL, "New ring is", ring)

                    if self.pid not in ring:
                        self.log(ERROR, "I'm not in the ring anymore. restarting...")
                        self.leader = None
                        self.neighbour_addr = None
                        self.ring = []
                        self.kv_cache = ThreadSafeKVCache()

                        self.broadcast_socket.sendto(
                            str.encode("JOIN: " + self.pid),
                            (BROADCAST_IP, BROADCAST_PORT),
                        )
                        continue

                    # Im still in the ring. continue normally
                    nb_pid = ring[ring.index(self.pid) - 1]
                    self.neighbour_addr = addr_from_pid(nb_pid)

                    if self.leader not in ring:
                        # we lost our leader -> election
                        self.start_election()

                elif m_type == "ELECTION":
                    self.log(ALL, "Received ELECTION message", message)
                    self.handle_election(json.loads(message))

                    # TODO: I think this doesnt work. Handle_election is only handeling a single message. With this, we replicate after every message in the election process
                    # for addr in self.ring:
                    # self.ring_socket.sendto(str.encode("REPLICATE: ", json.dumps(self.kv_cache)), addr)

                elif m_type == "CONNECT":
                    self.log(ALL, "Received CONNECT message", message)

                    # We only respond if we are the leader. This ensures that if there is currently no leader, the join will timeout
                    if self.leader == self.pid:
                        self.log(ALL, "Sending leader message")
                        self.broadcast_socket.sendto(
                            str.encode("LEADER: " + str(self.client_socket_addr)),
                            (BROADCAST_IP, BROADCAST_PORT),
                        )

                elif m_type == "LEADER":
                    # This is only from servers to clients
                    self.log(ALL, "Received LEADER message", message)

                elif m_type == "REPLICATE":
                    if self.leader == self.pid:
                        self.log(
                            ERROR, "Received REPLICATE message but I'm leader", message
                        )
                    else:
                        self.log(INFO, "Replicating data from leader", message)
                        data = json.loads(message)
                        self.kv_cache = ThreadSafeKVCache(data)

                        self.ring_socket.sendto(str.encode("REP_OK: " + self.pid), addr)

                elif m_type == "REP_OK":
                    self.log(ALL, "Received REP_OK message", message)
                    sender = message
                    if self.rep and self.replication:
                        if sender in self.replication:
                            self.replication.remove(sender)
                        else:
                            print("ring", self.replication)
                            self.log(
                                ERROR,
                                "Received REP_OK from addr that we no wait for",
                                sender,
                            )

                else:
                    self.log(ERROR, "Received unknown message", m_type, message)

    def send_join(self):
        self.broadcast_socket.sendto(
            str.encode("JOIN: " + self.pid), (BROADCAST_IP, BROADCAST_PORT)
        )

        # TODO: check timeout to create ring with just us if nothing exists
        # should work like this
        threading.Timer(JOIN_TIMEOUT, self.create_ring).start()

    def create_ring(self):
        """gets called if the initial JOIN message timed out which means there is no existing ring and we should create the initial one"""
        if not self.ring:
            self.ring = [self.pid]
            self.set_leader(self.pid)
            self.log(
                INFO,
                "Creating initial ring with just only us since our JOIN message timed out after",
                JOIN_TIMEOUT,
                "second(s)",
            )

            self.log(INFO, "Also creating a empty cache since Im the only one")
            self.kv_cache = ThreadSafeKVCache()

    # ELECTIONS

    def election_message(self, mid, isLeader):
        election_body = {"mid": mid, "isLeader": isLeader}
        return "ELECTION: " + json.dumps(election_body)

    def start_election(self):
        self.in_election = True

        self.log(INFO, "Starting election")

        if self.neighbour_addr:
            election_message = self.election_message(self.pid, False)
            self.log(ALL, "Sending election message to neighbour", self.neighbour_addr)
            self.log(ALL, "Election message:", election_message)
            self.ring_socket.sendto(election_message.encode(), self.neighbour_addr)

    def handle_election(self, election_body):
        if election_body["isLeader"]:
            if not self.in_election:
                return

            self.set_leader(election_body["mid"])
            self.in_election = False

            self.log(INFO, "new leader is", self.leader)

            if self.neighbour_addr:
                election_message = self.election_message(self.leader, True)
                self.ring_socket.sendto(election_message.encode(), self.neighbour_addr)

            return

        if election_body["mid"] < self.pid and not self.in_election:
            self.in_election = True

            self.log(ALL, "Proposing myself as leader")

            if self.neighbour_addr:
                election_message = self.election_message(self.pid, False)
                self.ring_socket.sendto(election_message.encode(), self.neighbour_addr)

        elif election_body["mid"] > self.pid:
            self.in_election = True

            self.log(ALL, "Forwarding election message")

            if self.neighbour_addr:
                election_message = self.election_message(election_body["mid"], False)
                self.ring_socket.sendto(election_message.encode(), self.neighbour_addr)

        elif election_body["mid"] == self.pid:
            self.set_leader(self.pid)
            self.in_election = False

            self.log(INFO, "I am the new leader and forwarding election message")

            if self.neighbour_addr:
                election_message = self.election_message(self.pid, True)
                self.ring_socket.sendto(election_message.encode(), self.neighbour_addr)

    # UTILS
    def close(self):
        """greacefully close the server with a LEAVE message to the neighbour"""

        self.log(INFO, "Closing server")
        self.running = False
        if self.neighbour_addr:
            self.log(INFO, "sending a LEAVE messge to my neighbour")
            self.ring_socket.sendto(
                str.encode("LEAVE: " + self.pid), self.neighbour_addr
            )
        else:
            self.log(INFO, "I have no neighbour to send a LEAVE message to")

        self.broadcast_socket.shutdown(socket.SHUT_RDWR)
        self.broadcast_socket.close()
        self.ring_socket.shutdown(socket.SHUT_RDWR)
        self.ring_socket.close()


if __name__ == "__main__":
    server = Server()
    threading.Thread(target=check_input, args=[server]).start()
    server.start()
