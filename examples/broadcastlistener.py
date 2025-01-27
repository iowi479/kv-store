import socket


if __name__ == "__main__":
    BROADCAST_PORT = 5973

    MY_HOST = socket.gethostname()
    MY_IP = socket.gethostbyname(MY_HOST)

    listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # dont use adress... use '' as default instead.
    # This way we receive broadcast messages from all interfaces(localhost as well)
    listen_socket.bind(("", BROADCAST_PORT))

    print("Listening to broadcast messages")

    while True:
        data, addr = listen_socket.recvfrom(1024)
        if data:
            print("Received broadcast message:", data.decode())
