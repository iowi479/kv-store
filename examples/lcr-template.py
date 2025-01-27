import socket
import json


my_uid = "127.0.0.1"
ring_port = 10001
leader_id = ""

# Buffer size
buffer_size = 1024

# Create a UDP socket
ring_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
ring_socket.bind((my_uid, ring_port))


neighbour = ("127.0.0.1", 10002)
participant = True

print("Participant is up and running at {}:{}".format(my_uid, ring_port))

while True:
    data, address = ring_socket.recvfrom(buffer_size)
    election_message = json.loads(data.decode())

    if election_message["isLeader"]:
        leader_uid = election_message["mid"]
        # forward received election message to left neighbour
        participant = False
        ring_socket.sendto(json.dumps(election_message).encode(), neighbour)

    if election_message["mid"] < my_uid and not participant:
        new_election_message = {"mid": my_uid, "isLeader ": False}
        participant = True
        # send received election message to left neighbour
        ring_socket.sendto(json.dumps(new_election_message).encode(), neighbour)
    elif election_message["mid"] > my_uid:
        # send received election message to left neighbour
        participant = True
        ring_socket.sendto(json.dumps(election_message).encode(), neighbour)
    elif election_message["mid"] == my_uid:
        leader_uid = my_uid
        new_election_message = {"mid": my_uid, "isLeader ": True}
        # send new election message to left neighbour
        participant = False
        ring_socket.sendto(json.dumps(new_election_message).encode(), neighbour)
