# Example of REQ -> ROUTER / DEALER -> ROUTER (replies).
# At each step, we show the message flow.

import zmq

context = zmq.Context()

# Set up sockets
req_socket = context.socket(zmq.REQ)
router_socket = context.socket(zmq.ROUTER)
dealer_socket = context.socket(zmq.DEALER)
router_socket2 = context.socket(zmq.ROUTER)

# Connect sockets
req_socket.connect("tcp://localhost:5555")
dealer_socket.connect("tcp://localhost:5556")
router_socket.bind("tcp://*:5555")
router_socket2.bind("tcp://*:5556")

# Set up poller
poller = zmq.Poller()
poller.register(req_socket, zmq.POLLIN)
poller.register(router_socket, zmq.POLLIN)
poller.register(dealer_socket, zmq.POLLIN)
poller.register(router_socket2, zmq.POLLIN)

# Send message
message = [b"Hello"]
print(f"REQ sent: {message}\n")
req_socket.send_multipart(message)

while True:
    # Wait for input from sockets
    sockets = dict(poller.poll())

    # If REQ socket has input
    if req_socket in sockets:
        # Receive the message and print it
        message = req_socket.recv_multipart()
        print(f"\nREQ received: {message}")
        break

    # If first ROUTER socket has input
    if router_socket in sockets:
        # Forward message to DEALER socket
        message = router_socket.recv_multipart()
        print(f"ROUTER1 -> DEALER1 -> ROUTER2: {message}")
        dealer_socket.send_multipart(message)

    # If DEALER socket has input
    if dealer_socket in sockets:
        # Forward message to REQ socket
        message = dealer_socket.recv_multipart()
        print(f"DEALER -> ROUTER1 -> REQ: {message}")
        router_socket.send_multipart(message)

    # If second ROUTER socket has input
    if router_socket2 in sockets:
        # Receive message from second ROUTER socket
        message = router_socket2.recv_multipart()

        # Add World to message
        message[-1] += b" World"

        print(f"ROUTER2 -> DEALER1: {message}")
        router_socket2.send_multipart(message)
