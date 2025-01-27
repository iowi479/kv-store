import socket


if __name__ == "__main__":
    BROADCAST_IP = "192.168.0.255"
    BROADCAST_PORT = 5973

    MY_HOST = socket.gethostname()
    MY_IP = socket.gethostbyname(MY_HOST)

    message = MY_IP + " sent a broadcast"

    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.sendto(str.encode(message), (BROADCAST_IP, BROADCAST_PORT))

    while True:
        data, addr = broadcast_socket.recvfrom(1024)
        if data:
            print("Received broadcast message:", data.decode())
            break

        # check for timeout here, if there is no previous ring
        if not data:
            print("No previous ring found. Creating a new one.")
            break

    broadcast_socket.close()

    # 1. Send a broadcast message to all participants in the network
    # 2. Listen to a response from a participant
    #   2.1. If a response with the current members is received, add yourself to the list of members
    #   2.2. If a response with the current members is not received, there is no current ring available. So create a new one.

    # 3. Start a new election in the new ring.
    #   3.1. If the ring is only us, we are the leader
    #   3.2. If there was a previous ring, the election determines the leader

    # 4. Wait for the election to finish
