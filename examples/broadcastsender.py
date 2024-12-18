import socket


def broadcast(ip, port, broadcast_message):
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.sendto(str.encode(broadcast_message), (ip, port))
    broadcast_socket.close()


if __name__ == '__main__':
    BROADCAST_IP = "192.168.0.255"
    BROADCAST_PORT = 5973

    MY_HOST = socket.gethostname()
    MY_IP = socket.gethostbyname(MY_HOST)

    message = MY_IP + ' sent a broadcast'
    broadcast(BROADCAST_IP, BROADCAST_PORT, message)

