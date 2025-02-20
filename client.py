import socket, threading, select, json, time
from utils import check_single_input, addr_from_pid
from conf import (
    BROADCAST_PORT,
    BROADCAST_IP,
    MY_IP,
    CONNECT_TIMEOUT,
    ALL,
    INFO,
    ERROR,
    LOGGING_LEVEL,
)


class Client:
    def log(self, level, *messages):
        if LOGGING_LEVEL >= level:
            print("[" + self.pid + "]", *messages)

    def __init__(self):
        # port is determined on tcp socket creation and updated then
        self.pid = MY_IP + ":" + "TBD"

        self.listening = True

        self.log(INFO, "Starting client")

        self.quit = False

    def connect(self):
        # setup socket for dynamic server discovery
        broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        broadcast_socket.bind(("", BROADCAST_PORT))

        connected = False
        while not connected:
            try:
                self.log(INFO, "Trying to connect")
                self.log(INFO, "Sending CONNECT message")
                broadcast_socket.sendto(
                    str.encode("CONNECT: " + self.pid), (BROADCAST_IP, BROADCAST_PORT)
                )

                # wait for answer to connect request until connected or timeout reached
                connect_time = time.time()
                while time.time() - connect_time < CONNECT_TIMEOUT and not connected:
                    try:
                        ready, _, _ = select.select(
                            [broadcast_socket], [], [], CONNECT_TIMEOUT
                        )

                        for s in ready:
                            data, _ = s.recvfrom(1024)

                            if data.index(b": ") == -1:
                                self.log(ERROR, "Received invalid message", data)
                                continue

                            [m_type, message] = data.decode().split(": ", 1)

                            if m_type == "CONNECT_OK":
                                # received answer to connect request
                                self.log(INFO, "Received CONNECT_OK message: ", message)
                                leader_addr = addr_from_pid(message)

                                # make TCP socket
                                self.client_socket = socket.socket(
                                    socket.AF_INET, socket.SOCK_STREAM
                                )
                                self.client_socket.setsockopt(
                                    socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
                                )
                                self.client_socket.bind(("", 0))

                                # update PID with the new port
                                self.pid = (
                                    MY_IP
                                    + ":"
                                    + str(self.client_socket.getsockname()[1])
                                )

                                # connect to leader server
                                self.log(INFO, "Connecting to leader", leader_addr)
                                self.client_socket.connect(leader_addr)

                                connected = True
                                self.listening = True

                            elif m_type == "CONNECT":
                                # dont do anything. This is from clients for servers and not for us
                                self.log(INFO, "Received CONNECT message: ", message)

                            else:
                                self.log(
                                    ERROR, "Received unknown message", m_type, message
                                )

                    except (socket.error, OSError) as e:
                        self.log(
                            ERROR,
                            "Error during store or receiving data:",
                            str(e),
                        )
                        break

            except Exception as e:
                self.log(ERROR, "Unexpected error during connection process:", str(e))
                break

        try:
            broadcast_socket.close()
        except (socket.error, OSError) as e:
            self.log(ERROR, "Error while closing broadcast socket:", str(e))

    def store(self, key, value):
        """handles the store command"""
        try:
            data_body = {"key": key, "value": value}

            self.client_socket.send(str.encode("STORE: " + json.dumps(data_body)))
            self.log(INFO, "Sent STORE message: ", data_body)

        except (socket.error, OSError) as e:
            self.log(ERROR, "Error while sending STORE message:", str(e))

    def retrieve(self, key):
        """handles the retrieve command"""
        try:
            data_body = {"key": key}
            self.client_socket.send(str.encode("RETRIEVE: " + json.dumps(data_body)))
            self.log(INFO, f"Sent RETRIEVE message: {key}")

        except (socket.error, OSError) as e:
            self.log(
                ERROR,
                "Error while sending RETRIEVE message:",
                str(e),
            )

    def listen(self):
        self.log(INFO, "Listening for messages now...")

        closed = False
        try:
            # listen for messages until the connection is closed or listening has been ordered to end otherwise
            while self.listening and not closed:
                ready, _, _ = select.select([self.client_socket], [], [])
                for s in ready:
                    data, _ = s.recvfrom(1024)

                    if not data:
                        self.log(ERROR, "No data received, socket may be closed")
                        closed = True
                        break

                    if data.index(b": ") == -1:
                        self.log(ERROR, "Received invalid message", data)
                        continue

                    [m_type, message] = data.decode().split(": ", 1)

                    if m_type == "DATA":
                        # answer to retieve command
                        self.log(INFO, "Received DATA message: ", message)

                        data = json.loads(message)

                        self.log(INFO, "Data received: ", data)
                        print("Data received: ", data)

                    elif m_type == "OK":
                        # answer to store command
                        self.log(INFO, "Received OK message: ", message)

                        data = json.loads(message)

                        self.log(INFO, "OK received: ", data)
                        print("STORE successfull: ", data)

                    else:
                        self.log(ERROR, "Received unknown message", m_type, message)
            self.client_socket.close()
        except (socket.error, OSError) as e:
            self.log(ERROR, "Error occurred while listening for messages", str(e))
            self.listening = False
            self.client_socket.close()

    def close(self):
        """handles graceful shutdown"""
        self.listening = False
        self.quit = True
        
        try:
            self.client_socket.shutdown(socket.SHUT_RDWR)
            self.client_socket.close()
        except (socket.error, OSError) as e:
            self.log(ERROR, "Error occurred while shutting down socket", str(e))


def handle_actions(client):
    """handles user input as actions until the clients closes"""

    while not client.quit:
        print("[CONFIG] Help: \n\t's' to [s]tore\n\t'r' to [r]retrieve\n\n")
        text = input()
        if text == "r":
            print("Enter key:")
            key = input().strip()
            client.retrieve(key)

        elif text == "s":
            print("Enter key:")
            key = input().strip()
            print("Enter value:")
            value = input().strip()
            client.store(key, value)

        else:
            check_single_input(text, client)
            print("Invalid option")


def reconnect(client, listen_thread):
    """handles disconnects until the clients closes"""
    while not client.quit:
        time.sleep(CONNECT_TIMEOUT)
        if not listen_thread.is_alive() and not client.quit:
            client.log(ALL, "Connection lost, reconnecting...")
            client.connect()
            listen_thread = threading.Thread(target=client.listen)
            listen_thread.start()


if __name__ == "__main__":
    client = Client()
    client.connect()

    # start listen thread
    listen_thread = threading.Thread(target=client.listen)
    listen_thread.start()

    # start reconnect thread
    connection_thread = threading.Thread(target=reconnect, args=(client, listen_thread))
    connection_thread.start()

    handle_actions(client)
