import socket, threading, select, json, time, uuid
from utils import addr_from_pid, check_input, ThreadSafeKVCache
from conf import (
    BROADCAST_PORT,
    BROADCAST_IP,
    MY_IP,
    JOIN_RETRY,
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
        self.joining = False

        self.in_election = False
        
        # global view
        self.ring = []
        self.neighbour_addr = None
        self.leader = None
        self.kv_cache = ThreadSafeKVCache()
        self.clients = []

        self.heartbeats = {}
        self.outstanding_acks = set()
        self.acks_received = set()

        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.broadcast_socket.bind(("", BROADCAST_PORT))

        # socket for communication on global view
        self.ring_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ring_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ring_socket.bind(("", 0))
        self.pid = MY_IP + ":" + str(self.ring_socket.getsockname()[1])

        self.log(INFO, "Started server")

        self.client_socket = None

        # timestamp of last replication
        self.rep_t = 0

    def start(self):
        listen_thread = threading.Thread(target=self.listen)
        listen_thread.start()
        
        hb_thread = threading.Thread(target=self.monitor_heartbeats)
        hb_thread.start()

        self.send_join()

    def send_reliable_threaded(self, data, addr, message_type):
        """run send_reliable() in a separate thread"""

        thread = threading.Thread(target=self.send_reliable, args=(data, addr, message_type))
        thread.daemon = True  # Daemon thread will exit if the main program stops
        thread.start()

    def send_reliable(self, data, addr, message_type, max_retries=5, timeout=3):
        """send a message reliably by waiting for an acknowledgment"""
        
        # create unique acknowledgement and store to acks to wait for
        ack = str(uuid.uuid4())
        self.outstanding_acks.add(ack)
        data["ack"] = ack
        message = str.encode(f"{message_type}: " + json.dumps(data))
        
        # send message upto max_tries times
        for attempt in range(max_retries):
            self.ring_socket.sendto(message , addr)
            self.log(INFO, f"Sent {message} to {addr}, waiting for ACK... (Attempt {attempt+1})")
            
            # wait some time to allow acknowledgment to arrive
            time.sleep(timeout)

            # end sending process if acknowledgment received
            if ack not in self.outstanding_acks:
                self.log(INFO, f"Message to {addr} delivered")
                return True

            self.log(INFO, f"No ACK received from {addr}, retrying...")

        # message delivery failed
        self.outstanding_acks.discard(ack)
        self.log(ERROR, f"Failed to deliver {message} to {addr} after {max_retries} attempts")
        return False  

    def set_leader(self, leader):
        """handles the declaration of a new leader"""
        
        if leader == self.pid:
            # if self is new leader start accepting clients via TCP socket
            self.leader = leader
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.client_socket.bind(("", 0))

            self.client_socket_addr = (
                MY_IP + ":" + str(self.client_socket.getsockname()[1])
            )

            self.listening_for_clients = True
            
            accept_thread = threading.Thread(target=self.listen_for_clients)
            accept_thread.daemon = True
            accept_thread.start()

            return

        if self.leader == self.pid:
            # if self was leader before stop listening for clients and shutdown connections
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
        """handles accepting new clients"""
        while self.listening_for_clients and self.client_socket and self.running:
            try:
                self.client_socket.listen(1)
                conn, addr = self.client_socket.accept()
                self.clients.append(conn)

                # start client handler thread for new client
                client_thread = threading.Thread(target=self.handle_client, args=[conn, addr])
                client_thread.daemon = True
                client_thread.start()

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

                    self.kv_cache.set(data["key"], data["value"])
                    
                    # replicate data to replica servers
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
        
        # set time stamp of replication
        self.rep_t = time.time()
        replication = self.ring.copy()

        # we remove ourself from the replication
        # since we already have the data
        replication.remove(self.pid)

        replication_data = {
            "rep_t": self.rep_t,
            "data": self.kv_cache.get_dict()
        }

        for addr in replication:
            if addr == self.pid:
                continue

            self.send_reliable_threaded(
                replication_data,
                addr_from_pid(addr),
                "REPLICATE"
            )

    def heartbeat_timout(self, pid):
        """handles heartbeat timeout"""

        self.log(ERROR, "Heartbeat timeout for", pid)
        
        # remove time out server's PID from global view
        if pid in self.ring:
            self.ring.remove(pid)
        self.heartbeats.pop(pid, None)

        # propagate update
        for addr in self.ring:
            update_data = {
                "ring": self.ring.copy()
            }

            self.send_reliable_threaded(
                update_data,
                addr_from_pid(addr),
                "UPDATE"
            )

    def clear_heartbeats(self):
        """This clears old hbs for left PIDs"""
        for pid in list(self.heartbeats):
            if pid not in self.ring:
                self.heartbeats.pop(pid)

    def monitor_heartbeats(self):
        """handles heartbeats"""
        while self.running:
            time.sleep(1)
            for pid in self.ring:
                if pid == self.pid:
                    continue

                elif pid not in self.heartbeats:
                    # no heartbeat yet for server with pid
                    # send first heartbeat
                    self.heartbeats[pid] = {"last_hb": 0, "sent_ping": time.time()}
                    self.ring_socket.sendto(
                        str.encode("PING: " + json.dumps({"sender": self.pid})),
                        addr_from_pid(pid),
                    )

                elif not self.heartbeats[pid]["sent_ping"]:
                    # not waiting for heartbeat response of server with pid
                    if (
                        time.time() - self.heartbeats[pid]["last_hb"]
                        > HEARTBEAT_INTERVAL
                    ):
                        # sent new heartbeat if last heartbeat happened HEARTBEAT_INTERVAL time ago
                        self.heartbeats[pid]["sent_ping"] = time.time()
                        self.ring_socket.sendto(
                            str.encode("PING: " + json.dumps({"sender": self.pid})),
                            addr_from_pid(pid),
                        )

                else:
                    # waiting for heartbeat response of server with pid
                    if (
                        time.time() - self.heartbeats[pid]["sent_ping"]
                        > HEARTBEAT_TIMEOUT
                    ):
                        self.heartbeat_timout(pid)
                        continue

    def listen(self):
        self.log(INFO, "Listening for messages now...")

        while self.running:

            # listen for messages with timeout
            ready, _, _ = select.select(
                [self.ring_socket, self.broadcast_socket], [], [], HEARTBEAT_TIMEOUT
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
                    # accept new server
                    self.log(ALL, "Received ACCEPT message with ring: ", message)
                    self.joining = False
                    data_message = json.loads(message)
                    
                    ack = data_message["ack"]
                    self.ring_socket.sendto(str.encode("ACK: " + ack), addr)

                    #only act on message if no duplicate
                    if ack not in self.acks_received:
                        self.acks_received.add(ack)
                        
                        # update global view and start message
                        self.ring = data_message["ring"]
                        nb_pid = self.ring[self.ring.index(self.pid) - 1]
                        self.neighbour_addr = addr_from_pid(nb_pid)

                        self.start_election()

                elif m_type == "JOIN":
                    self.log(ALL, "Received JOIN message", message)

                    # accept new server if leader
                    if self.leader == self.pid:
                        # update global view
                        self.ring.append(message)
                        no_duplicates = list(set(self.ring))
                        self.ring = no_duplicates
                        self.ring.sort()

                        # replicate key-value store to new server
                        replication_thread = threading.Thread(target=self.replicate_data)
                        replication_thread.start()

                        # propagate update
                        for addr in self.ring:
                            accept_data = {
                                "ring": self.ring.copy()
                            }

                            self.send_reliable_threaded(
                                accept_data,
                                addr_from_pid(addr),
                                "ACCEPT"
                            )

                elif m_type == "LEAVE":
                    self.log(ALL, "Received LEAVE message", message)
                    
                    if message in self.ring:
                        self.ring.remove(message)

                    # propagate updated global view
                    for addr in self.ring:
                        update_data = {
                            "ring": self.ring.copy()
                        }

                        self.send_reliable_threaded(
                            update_data,
                            addr_from_pid(addr),
                            "UPDATE"
                        )

                elif m_type == "PING":
                    # received heartbeat
                    self.log(ALL, "Received PING message", message)
                    payload = json.loads(message)
                    payload["responder"] = self.pid
                    self.broadcast_socket.sendto(
                        str.encode("PING_RESPONSE: " + json.dumps(payload)), addr
                    )

                elif m_type == "PING_RESPONSE":
                    # received heartbeat response
                    self.log(ALL, "Received PING_RESPONSE message", message)
                    payload = json.loads(message)

                    assert self.pid == payload["sender"]
                    responder = payload["responder"]

                    self.heartbeats[responder]["last_hb"] = time.time()
                    self.heartbeats[responder]["sent_ping"] = None

                elif m_type == "UPDATE":
                    self.log(ALL, "Received UPDATE message with ring: ", message)
                    data_message = json.loads(message)
                    
                    ack = data_message["ack"]
                    self.ring_socket.sendto(str.encode("ACK: " + ack), addr)
                    
                    #only act if no duplicate
                    if ack not in self.acks_received:
                        self.acks_received.add(ack)
                        ring = data_message["ring"]
                        
                        # update global view
                        self.ring = ring
                        self.clear_heartbeats()
                        self.log(ALL, "New ring is", ring)

                        if self.pid not in ring:
                            self.log(ERROR, "I'm not in the ring anymore. restarting...")
                            self.leader = None
                            self.neighbour_addr = None
                            self.ring = []
                            self.kv_cache = ThreadSafeKVCache()

                            self.send_join()
                            continue

                        # im still in the ring. continue normally
                        nb_pid = ring[ring.index(self.pid) - 1]
                        self.neighbour_addr = addr_from_pid(nb_pid)

                        if self.leader not in ring:
                            # we lost our leader -> election
                            self.start_election()

                elif m_type == "ELECTION":
                    self.log(ALL, "Received ELECTION message", message)

                    data_message = json.loads(message)

                    ack = data_message["ack"]
                    self.ring_socket.sendto(str.encode("ACK: " + ack), addr)
                    
                    #only act on message if no duplicate
                    if ack not in self.acks_received:
                        self.acks_received.add(ack)
                        self.handle_election(data_message)

                elif m_type == "CONNECT":
                    # received connection request from client
                    self.log(ALL, "Received CONNECT message", message)

                    # we only respond if we are the leader. This ensures that if there is currently no leader, the join will timeout
                    if self.leader == self.pid:
                        self.log(ALL, "Sending leader message")
                        self.broadcast_socket.sendto(
                            str.encode("CONNECT_OK: " + str(self.client_socket_addr)),
                            (BROADCAST_IP, BROADCAST_PORT),
                        )

                elif m_type == "CONNECT_OK":
                    # this is only from servers to clients
                    self.log(ALL, "Received CONNECT_OK message", message)

                elif m_type == "REPLICATE":
                    if self.leader == self.pid:
                        self.log(
                            ERROR, "Received REPLICATE message but I'm leader", message
                        )
                    else:
                        self.log(INFO, "Replicating data from leader", message)
                        data_message = json.loads(message)
                        
                        ack = data_message["ack"]
                        self.ring_socket.sendto(str.encode("ACK: " + ack), addr)

                        #only act on message if no duplicate
                        if ack not in self.acks_received:
                            self.acks_received.add(ack)

                            # only update key-value store if new timestamp is newer
                            if self.rep_t < data_message["rep_t"]:
                                # update key-value store and current timestamp
                                self.rep_t = data_message["rep_t"]
                                self.kv_cache = ThreadSafeKVCache(data_message["data"])

                elif m_type == "ACK":
                    self.log(INFO, "Received ACK", message)
                    self.outstanding_acks.discard(message)

                else:
                    self.log(ERROR, "Received unknown message", m_type, message)

    def send_join(self):
        """handles joining a ring"""
        
        # try to join JOIN_RETRY times to join the system
        self.joining = True
        for _ in range(JOIN_RETRY):
            if self.joining:
                self.log(INFO, "Trying to join...")
                self.broadcast_socket.sendto(
                    str.encode("JOIN: " + self.pid), (BROADCAST_IP, BROADCAST_PORT)
                )
            else:
                return
            time.sleep(3)

        # create ring with only this server if no answer after 5 tries with 3 second delay
        if self.joining:
            self.joining = False
            self.create_ring()

    def create_ring(self):
        """gets called if the initial JOIN message timed out which means there is no existing ring and we should create the initial one"""
        if not self.ring:
            self.ring = [self.pid]
            self.set_leader(self.pid)
            self.log(
                INFO,
                "Creating initial ring with just only us since our JOIN message timed out after",
                JOIN_RETRY,
                "tries with 3 second delay",
            )

            self.log(INFO, "Also creating a empty cache since Im the only one")
            self.kv_cache = ThreadSafeKVCache()

    # ELECTIONS

    def election_message(self, mid, isLeader):
        # mid = highest PID seen, isLeader indicates whether this is a leader announcement
        election_body = {"mid": mid, "isLeader": isLeader}
        return election_body

    def start_election(self):
        self.in_election = True

        self.log(INFO, "Starting election")

        # send ELECTION message to neighbor
        if self.neighbour_addr:
            election_message = self.election_message(self.pid, False)
            self.log(ALL, "Sending election message to neighbour", self.neighbour_addr)
            self.log(ALL, "Election message:", election_message)
            self.send_reliable_threaded(election_message, self.neighbour_addr, "ELECTION")

    def handle_election(self, election_body):
        # handle leader announcment
        if election_body["isLeader"]:
            if not self.in_election:
                return

            # set new leader
            self.set_leader(election_body["mid"])
            self.in_election = False

            self.log(INFO, "new leader is", self.leader)

            # propagate leader announcement
            if self.neighbour_addr:
                election_message = self.election_message(self.leader, True)
                self.send_reliable_threaded(election_message, self.neighbour_addr, "ELECTION")

            return

        if election_body["mid"] < self.pid and not self.in_election:
            # propose server as new leader if seen PID is lower
            self.in_election = True

            self.log(ALL, "Proposing myself as leader")

            if self.neighbour_addr:
                election_message = self.election_message(self.pid, False)
                self.send_reliable_threaded(election_message, self.neighbour_addr, "ELECTION")

        elif election_body["mid"] > self.pid:
            # forward election message if server's PID is lower than seen PID
            self.in_election = True

            self.log(ALL, "Forwarding election message")

            if self.neighbour_addr:
                election_message = self.election_message(election_body["mid"], False)
                self.send_reliable_threaded(election_message, self.neighbour_addr, "ELECTION")

        elif election_body["mid"] == self.pid:
            # if seen PID is server's PID message wrapped around, announce server as leader
            self.set_leader(self.pid)
            self.in_election = False

            self.log(INFO, "I am the new leader and forwarding election message")

            if self.neighbour_addr:
                election_message = self.election_message(self.pid, True)
                self.send_reliable_threaded(election_message, self.neighbour_addr, "ELECTION")

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

        try:
            # shutdown and close the broadcast socket
            if self.broadcast_socket:
                self.broadcast_socket.shutdown(socket.SHUT_RDWR)
                self.broadcast_socket.close()
            
            # shutdown and close the ring socket
            if self.ring_socket:
                self.ring_socket.shutdown(socket.SHUT_RDWR)
                self.ring_socket.close()

        except socket.error as e:
            self.log(ERROR, "Problem with shutting down sockets")
        
        self.log(INFO, "Server closed")


if __name__ == "__main__":
    server = Server()
    
    input_thread = threading.Thread(target=check_input, args=[server])
    input_thread.start()
    
    server.start()
