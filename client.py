import socket
import threading
import select
import os
import json
import sys
import time

ALL = 3
INFO = 2
ERROR = 1
NONE = 0

LOGGING_LEVEL = INFO
BROADCAST_PORT = 5000
BROADCAST_IP = "192.168.0.255"
MY_IP = os.environ['MY_IP']
MY_PORT = int(os.environ['MY_PORT'])

class Client:
    def log(self, level, *messages):
        if LOGGING_LEVEL >= level:
            print("[" + self.pid + "]", *messages)
    
    def __init__(self):
        self.JOIN_TIMEOUT = 5
        self.pid = MY_IP + ":" + str(MY_PORT)
        
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        self.log(INFO, "Starting client")

    def connect(self):
        try:
            broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            broadcast_socket.bind(('', BROADCAST_PORT))
        except socket.error as e:
            self.log(ERROR, "Socket error while creating broadcast socket:", str(e))
            return

        connected = False
        while not connected:
            try:
                self.log(INFO, "Trying to connect")
                broadcast_socket.sendto(str.encode("CONNECT: " + self.pid), (BROADCAST_IP, BROADCAST_PORT))
                connect_time = time.time()

                while time.time() - connect_time < self.JOIN_TIMEOUT and not connected:
                    try:
                        ready, _, _ = select.select([broadcast_socket], [], [], self.JOIN_TIMEOUT)
                        for s in ready:
                            data, addr = s.recvfrom(1024)

                            if data.index(b": ") == -1:
                                self.log(ERROR, "Received invalid message", data)
                                continue

                            [m_type, message] = data.decode().split(": ", 1)

                            # TODO: handle multiple messages to improve consistency
                            if m_type == "LEADER":
                                self.log(INFO, "Received LEADER message: ", message)
                                leader_msg = json.loads(message)
                                try:
                                    self.client_socket.connect((leader_msg['ip'], leader_msg['port']))
                                    self.log(ALL, "Connected to: ", (leader_msg['ip'], leader_msg['port']))
                                    connected = True
                                except socket.error as e:
                                    self.log(ERROR, "Socket error while connecting to leader:", str(e))
                            else:
                                self.log(ERROR, "Received unknown message", m_type, message)

                    except socket.error as e:
                        self.log(ERROR, "Socket error during select or receiving data:", str(e))
                        break

            except Exception as e:
                self.log(ERROR, "Unexpected error during connection process:", str(e))
                break

        try:
            broadcast_socket.close()
        except socket.error as e:
            self.log(ERROR, "Error while closing broadcast socket:", str(e))



    def store(self, key, value):
        try:
            data_body = {
                'key': key,
                'value': value
            }

            # Send data over the socket
            self.client_socket.send("STORE: " + json.dumps(data_body)) 
            self.log(INFO, f"Sent STORE message: {data_body}")

        except socket.error as e:
            # Handle socket-related errors (e.g., connection issues)
            self.log(ERROR,f"Socket error while sending STORE message: {e}")

        except json.JSONDecodeError as e:
            # Handle errors during JSON encoding
            self.log(ERROR,f"JSON error while encoding STORE message: {e}")

        except Exception as e:
            # Handle any other unexpected errors
            self.log(ERROR,f"Unexpected error while sending STORE message: {e}")

    def retrieve(self, key):
        try:
            self.client_socket.send("RETRIEVE: " + key) 
            self.log(INFO,f"Sent RETRIEVE message: {key}")

        except socket.error as e:
            # Handle socket-related errors (e.g., connection issues)
            self.log(ERROR,f"Socket error while sending RETRIEVE message: {e}")

        except Exception as e:
            # Handle any other unexpected errors
            self.log(ERROR,f"Unexpected error while sending RETRIEVE message: {e}")


    def listen(self):
        self.log(INFO, "Listening for messages now...")

        while True:
            try:
                # This Line did it... A tribute for 2h of debugging.
                ready, _, _ = select.select([self.client_socket], [], [])
                for s in ready:
                    data, addr = s.recvfrom(1024)

                    if not data:
                        self.log(ERROR, "No data received, socket may be closed")
                        break  # Exit the loop if no data is received (indicating a closed socket)


                    if data.index(b": ") == -1:
                        self.log(ERROR, "Received invalid message", data)
                        continue

                    [m_type, message] = data.decode().split(": ", 1)

                    if m_type == "RETRIEVE":
                        self.log(INFO, "Received RETRIEVE message: ", message)

                    else:
                        self.log(ERROR, "Received unknown message", m_type, message)
            
            except socket.error as e:
                self.log(ERROR, "Socket error occurred", str(e))
                break  # Exit the loop if the socket is closed or there's an error

            except OSError as e:
                self.log(ERROR, "OS error occurred", str(e))
                break  # Exit the loop if there's an OS error

            except Exception as e:
                self.log(ERROR, "Unexpected error occurred", str(e))
                break  # Exit the loop for any unexpected errors

        self.client_socket.close()
    
    def close(self):
        self.client_socket.close()

def check_input():
    print("[CONFIG] Help: \n\t'q' to [q]uit\n\t'c' to [c]rash\n\t'v' to toggle [v]erbosity\n\n")
    while True:
        text = input()
        if text == "q":
            print("[CONFIG] [q]uitting")
            client.close()
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
    
    client = Client()
    client.connect()
    listen_thread = threading.Thread(target=client.listen)
    listen_thread.start()

    while True:
        if listen_thread.is_alive():
            print("[CONFIG] Help: \n\t's' to [s]tore\n\t'r' to [r]retrieve\n\n")
            text = input()
            if text == "r":
                print("Enter key:")
                key = input()
                client.retrieve(key)
            elif text == "s":
                print("Enter key:")
                key = input()
                print("Enter value:")
                value = input()
                client.store(key,value)
            else:
                print("Invalid option")
        else:
            client.log(ALL, "Connection lost, reconnecting...")
            client.connect()
            listen_thread = threading.Thread(target=client.listen)
            listen_thread.start()
    