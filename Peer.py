import socket
import threading
import uuid
import time

BROADCAST_PORT = 9999  # Dedicated port for UDP broadcasts

class Peer():
    def __init__(self, peer_id, address, port):
        self.peer_id = peer_id
        self.address = address
        self.partOfNetwork = False
        self.port = port
        self.connection_dict = {}
        self.isGroupLeader = False
        self.tcp_thread = None
        self.udp_thread = None
        self.groupView = {}
        self.orderedPeerList = []
        self.successor = None  # Changed to dict: {'peer_id':, 'address':, 'port':}
        self.election_active = False
        self.leader_id = None
        self.last_leader_heartbeat = time.time()
        self.heartbeat_interval = 5  # seconds
        self.heartbeat_timeout = 10  # seconds
        self.heartbeat_thread = None
        self.leader_check_thread = None
        self.pending_acks = {}

    def __str__(self):
        return f"Peer ID: {self.peer_id}, Address: {self.address}, Port: {self.port}"
    
    def start(self):
        self.start_listening_threads()
        self.broadcast_new_peer_request('127.0.0.1', 9999)
        time_after_broadcast = time.time()
        #how does a peer know it has been added to the group?
        #maybe once it receives successor information from leader?
        # if no reply is received within 5 seconds, assume leader
        time.sleep(5)  # wait for potential responses
        while not self.partOfNetwork:
            if time.time() - time_after_broadcast > 5:
                print("No response received. Assuming role of Group Leader.")
                self.isGroupLeader = True
                self.leader_id = self.peer_id
                self.partOfNetwork = False
                self.groupView[self.peer_id] = {
                    'address': self.address,
                    'port': self.port,
                    'role': 'leader'
                }
                self.orderedPeerList.append(self.peer_id)
                break

        #CONTINUE BROADCASTING UNTIL PART OF NETWORK OR DISCOVER ANOTHER PEER
        while not self.partOfNetwork:
            print("Waiting to join the network...")
            self.broadcast_new_peer_request('127.0.0.1', 9999)
            time.sleep(5)
        
        self.start_lcr_election()

        # Start fault tolerance threads
        self.start_leader_check_thread()
        if self.isGroupLeader:
            self.start_heartbeat_thread()

        # Keep the peer running by joining the threads
        if self.tcp_thread:
            self.tcp_thread.join()
        if self.udp_thread:
            self.udp_thread.join()



    def connect(self):
        print(f"Connecting to Peer {self.peer_id} at {self.address}:{self.port}")   

    def create_listening_socket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('0.0.0.0', self.port))
        s.listen()
        return s
    
    def accept_connection(self, listening_socket):
        while True:
            conn, addr = listening_socket.accept()
            msg = conn.recv(1024)  # Optional handshake message
            decoded_msg = msg.decode()
            # Parse: "HELLO from 127.0.0.1:5001 and peer id:peer2"
            if " and peer id:" in decoded_msg:
                parts = decoded_msg.split(" and peer id:")
                address_port = parts[0].split(" from ")[1]  # "127.0.0.1:5001"
                peer_id = parts[1]
                peer_ip, peer_port_str = address_port.split(":")
                peer_port = int(peer_port_str)
            else:
                # Fallback, but should not happen
                peer_port = 0
                peer_id = "unknown"
            print(f"Accepted connection from {addr} with port {peer_port} and peer id:{peer_id}")
            # self.connection_dict[f"peer_{peer_port}"] = conn
            self.connection_dict[peer_id] = conn

            threading.Thread(target=self.receive_message,
                             args=(peer_id,),
                             daemon=True).start()
        
    
    def send_message(self, peer_key, message):
        if peer_key in self.connection_dict:
            conn = self.connection_dict[peer_key]
            conn.sendall(message.encode())
            print(f"Sent to {peer_key}: {message}")
        else:
            print(f"No connection found for {peer_key} while sending message only {self.connection_dict.keys()} available")

    def receive_message(self, peer_key, buffer_size=1024):
        print(f"Receive thread started for {peer_key}")
        if peer_key in self.connection_dict:
            conn = self.connection_dict[peer_key]
            while True:
                try:
                    data = conn.recv(buffer_size)
                    if not data:
                        print(f"Connection closed by {peer_key}")
                        break
                    message = data.decode()
                    print(f"Received from {peer_key}: {message}")
                    if message.startswith("SUCCESSOR_INFORMATION:"):
                        parts = message.split("SUCCESSOR_INFORMATION:")[1].split(":", 1)
                        successor_list_str = parts[0]
                        group_view_str = parts[1]
                        successor_list = eval(successor_list_str)
                        group_view = eval(group_view_str)
                        self.handle_successor_information(successor_list, group_view)
                        self.partOfNetwork = True   #ONLY AFTER RECEIVING SUCCESSOR INFO DO WE CONSIDER OURSELVES PART OF THE NETWORK
                        print(f"Peer {self.peer_id} is now part of the network.")
                        print("successor information processed. ", self.successor)
                    elif message.startswith("LCR_ELECTION:"):
                        print(f"{self.peer_id} received LCR election message and {self.connection_dict.keys()}")
                        self.handle_lcr_election(message)
                    elif message.startswith("LCR_LEADER:"):
                        self.handle_lcr_leader(message)
                    elif message == "HEARTBEAT":
                        self.last_leader_heartbeat = time.time()
                        self.send_message(peer_key, "HEARTBEAT_ACK")
                    elif message == "HEARTBEAT_ACK":
                        # Leader received ack, no action needed
                        pass
                except Exception as e:
                    print(f"Error receiving from {peer_key}: {e}")
                    break
        else:
            print(f"No connection found for {peer_key} while receiving message")

    def send_connection_request(self, peer_ip, peer_port, peer_id):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((peer_ip, peer_port))  # TCP handshake
            # Optional handshake message
            sock.send(b"HELLO from " + f"{self.address}:{self.port} and peer id:{self.peer_id}".encode())
            # Store the connection
            # self.connections[f"{peer_ip}:{peer_port}"] = sock
            # self.connection_dict[f"peer_{peer_port}"] = sock
            self.connection_dict[peer_id] = sock
            print(f"Connected to id : {peer_id} and {peer_ip}:{peer_port}")
            # Start receive thread for this connection
            threading.Thread(target=self.receive_message, args=(peer_id,), daemon=True).start()
        except socket.error as e:
            print("Connection failed:", e)
            raise  # Re-raise the exception so caller can handle it

    #SEND TO NEW NODE BROADCAST REQUESTS FOR NEW PEERS: CREATE NEW UDP SOCKET 
    #IN ORDER TO GAIN ACCESS TO THE NETWORK
    
    def broadcast_new_peer_request(self, broadcast_ip, broadcast_port, message="NEW_PEER_REQUEST"):
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Don't set SO_BROADCAST for localhost - send directly to 127.0.0.1
        message = f"{message}:{self.peer_id}:{self.port}"
        udp_sock.sendto(message.encode(), (broadcast_ip, broadcast_port))
        print(f"Broadcasted new peer request to {broadcast_ip}:{broadcast_port}")
        udp_sock.close()

    def listen_for_broadcasts(self, buffer_size=1024):
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_sock.bind(('127.0.0.1', BROADCAST_PORT))
        print(f"Listening for broadcasts on port {BROADCAST_PORT}")
        while True:
            data, addr = udp_sock.recvfrom(buffer_size)
            decoded_msg = data.decode()
            parts = decoded_msg.split(":")
            new_peer_id = parts[1]
            peer_port = int(parts[2])
            print(f"Received broadcast from {addr}: {parts[0]} with Peer ID {new_peer_id} on port {peer_port}")
            # Process the broadcast message as needed
            # For example, add the new peer to the group view

            if self.isGroupLeader:
                if decoded_msg.split(':')[0] == "NEW_PEER_REQUEST":
                    if new_peer_id not in self.groupView:
                        self.groupView[new_peer_id] = {
                            'address': addr[0],
                            'port': peer_port,
                            'role': 'member'   
                        }
                        print(f"Updated group view: {self.groupView}")

                        #add to peer successor list
                        self.orderedPeerList.append(new_peer_id)
                        #reorder peer list if group leader
                        self.orderedPeerList = sorted(self.orderedPeerList)
                        print(f"Updated ordered peer list: {self.orderedPeerList}")
                        #broadcast_successor_information()
                        
                        
                        if(len(self.orderedPeerList) == 2):
                            self.handle_successor_information(self.orderedPeerList, self.groupView)

                        self.send_successor_information(new_peer_id) 
                        #HOW TO FIGURE OUT WHICH PEERS TO SEND NEW SUCCESSOR INFO TO?

                    else:
                        print(f"Peer ID {new_peer_id} already in group view.")
            

    def send_successor_information(self, peer_id_to_send_info):
        if self.isGroupLeader:
            message = f"SUCCESSOR_INFORMATION:{self.orderedPeerList}:{self.groupView}"
            peer_info = self.groupView.get(peer_id_to_send_info)
            print(f"Sending successor information to {peer_id_to_send_info} at {peer_info}")
            time.sleep(1)  # Give the peer time to start listening
            try:
                self.send_connection_request(peer_info['address'], peer_info['port'], peer_id_to_send_info)
                self.send_message(peer_id_to_send_info, message)
                print(f"Sent successor information to {peer_id_to_send_info}: {message}")

                self.partOfNetwork = True  # Now part of the network after sending successor info
                print(f"Peer {self.peer_id} itself is now part of the network.")
            except Exception as e:
                print(f"Failed to send successor information to {peer_id_to_send_info}: {e}")

            # tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # peer_info = self.groupView.get(peer_id_to_send_info)
            # if peer_info:
            #     try:
            #         tcp_sock.connect((peer_info['address'], peer_info['port']))
            #         tcp_sock.sendall(message.encode())
            #         print(f"Sent successor information to {peer_id_to_send_info}: {message}")
            #         self.connection_dict[f"peer_{peer_info['port']}"] = tcp_sock
            #     except socket.error as e:
            #         print(f"Failed to send successor information to {peer_id_to_send_info}: {e}")
            #     finally:
            #         tcp_sock.close()

                                               
    def handle_successor_information(self, successor_list, group_view):
        #[1,2,3,4]
        print(f"Received successor information: {successor_list}")
        # Update internal state with successor information
        self.orderedPeerList = successor_list
        print(f"Updated ordered peer list: {self.orderedPeerList}")

        selfIndex = self.orderedPeerList.index(self.peer_id)
        successorIndex = (selfIndex + 1) % len(self.orderedPeerList)
        successor_id = self.orderedPeerList[successorIndex]
        successor_info = group_view.get(successor_id)
        if successor_info:
            self.successor = {
                'peer_id': successor_id,
                'address': successor_info['address'],
                'port': successor_info['port']
            }
        else:
            self.successor = successor_id  # Fallback
        print(f"My successor is now: {self.successor}")


    def start_listening_threads(self):
        tcp_listening_socket = self.create_listening_socket()
        self.tcp_thread = threading.Thread(target=self.accept_connection,args=(tcp_listening_socket,), daemon=True)
        self.tcp_thread.start()
        
        #IF group leader not known and partOfNetwork is false, start UDP listener also
        if not self.isGroupLeader and not self.partOfNetwork:
            print("UDP THREAD STARTED FOR NON-GROUP LEADER")
            udp_thread = threading.Thread(target=self.listen_for_broadcasts, daemon=True)
            udp_thread.start()

        if self.isGroupLeader:
            print("UDP THREAD STARTED FOR GROUP LEADER")
            udp_thread = threading.Thread(target=self.listen_for_broadcasts, daemon=True)
            udp_thread.start()

        print("TCP and UDP listeners started.")

    def start_lcr_election(self):
        if not self.election_active and self.successor:
            print(f"{self.peer_id} Now starting LCR Election")
            self.election_active = True
            self.isGroupLeader = False
            message = f"LCR_ELECTION:{self.peer_id}"
            successor_id = self.successor['peer_id'] if isinstance(self.successor, dict) else self.successor
            self.send_message(successor_id, message)

    def handle_lcr_election(self, message):
        print(f"Handling LCR election message: {message}")
        received_id = message.split(":")[1]
        if received_id > self.peer_id:
            # A higher ID is circulating, relinquish leadership
            self.isGroupLeader = False
            print(f"Relinquishing leadership to higher ID: {received_id}")
            # forward to successor
            successor_id = self.successor['peer_id'] if isinstance(self.successor, dict) else self.successor
            self.send_message(successor_id, message)
        elif received_id < self.peer_id:
            # discard
            pass
        elif received_id == self.peer_id:
            # I am the leader
            self.isGroupLeader = True
            self.leader_id = self.peer_id
            self.election_active = False
            print(f"I am the leader: {self.peer_id}")
            # announce leader
            leader_message = f"LCR_LEADER:{self.peer_id}"
            successor_id = self.successor['peer_id'] if isinstance(self.successor, dict) else self.successor
            self.send_message(successor_id, leader_message)

    def handle_lcr_leader(self, message):
        leader_id = message.split(":")[1]
        self.leader_id = leader_id   #update known leader

        #forward the leader announcement to other successors
        if leader_id != self.peer_id:
            # forward to successor
            successor_id = self.successor['peer_id'] if isinstance(self.successor, dict) else self.successor
            self.send_message(successor_id, message)
        else:
            # election complete
            self.election_active = False
            print("Election complete.")

    def start_heartbeat_thread(self):
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeats, daemon=True)
        self.heartbeat_thread.start()

    def send_heartbeats(self):
        while True:
            time.sleep(self.heartbeat_interval)
            if self.isGroupLeader:
                for peer_id in list(self.groupView.keys()):
                    if peer_id != self.peer_id:
                        try:
                            self.send_message(peer_id, "HEARTBEAT")
                        except Exception as e:
                            print(f"Failed to send heartbeat to {peer_id}: {e}")
                            self.handle_peer_failure(peer_id)

    def handle_peer_failure(self, peer_id):
        if peer_id in self.groupView:
            del self.groupView[peer_id]
            if peer_id in self.orderedPeerList:
                self.orderedPeerList.remove(peer_id)
            print(f"Peer {peer_id} failed, updated group: {self.groupView}, ordered: {self.orderedPeerList}")
            # Send updated successor info to remaining peers
            for pid in self.orderedPeerList:
                if pid != self.peer_id:
                    self.send_successor_information(pid)

    def start_leader_check_thread(self):
        self.leader_check_thread = threading.Thread(target=self.check_leader_heartbeat, daemon=True)
        self.leader_check_thread.start()

    def check_leader_heartbeat(self):
        while True:
            time.sleep(1)
            if not self.isGroupLeader and time.time() - self.last_leader_heartbeat > self.heartbeat_timeout:
                print("Leader heartbeat timeout, starting election")
                self.start_lcr_election()
                break  # Prevent multiple elections