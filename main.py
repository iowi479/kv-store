import socket


def broadcast(ip, port, broadcast_message):
    # Create a UDP socket
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)  # UDP
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    # Send message on broadcast address
    broadcast_socket.sendto(str.encode(broadcast_message), (ip, port))
    broadcast_socket.close()


if __name__ == '__main__':
    # Broadcast address and port
    BROADCAST_IP = "192.168.0.255"
    BROADCAST_PORT = 3000

    # Local host information
    MY_HOST = socket.gethostname()
    MY_IP = socket.gethostbyname(MY_HOST)

    # Send broadcast message
    message = MY_IP + ' sent a broadcast'
    broadcast(BROADCAST_IP, BROADCAST_PORT, message)

