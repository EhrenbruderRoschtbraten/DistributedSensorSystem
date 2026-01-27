import Peer
import multiprocessing
import socket
import time
import identity
import Peer_utils


#initialize two peers
#start peer1 listening socket in a separate process
def peer1stuff(peer_id1, peer1address, peer1port):
        # Create peer instance in this process
        peer1 = Peer.Peer(peer_id=peer_id1, address=peer1address, port=peer1port)
        peer1.start()


        



#start peer2 listening socket in a separate process
def peer2stuff(peer_id2, peer2address, peer2port):
        # Create peer instance in this process
        peer2 = Peer.Peer(peer_id=peer_id2, address=peer2address, port=peer2port)
        peer2.start()

#start peer2 listening socket in a separate process
def peer3stuff(peer_id3, peer3address, peer3port):
        # Create peer instance in this process
        peer3 = Peer.Peer(peer_id=peer_id3, address=peer3address, port=peer3port)
        peer3.start()


if __name__ == '__main__':

    path1 = "peer1_identity/peer1_identity.txt"
    peer_id1 = identity.load_or_generate_peer_id(PEER_ID_FILE=path1)
    peer1address = Peer_utils.get_local_ip()
    peer1port = Peer_utils.get_free_port()
     
    path2 = "peer2_identity/peer2_identity.txt"
    peer_id2 = identity.load_or_generate_peer_id(PEER_ID_FILE=path2)
    peer2address = Peer_utils.get_local_ip()
    peer2port = Peer_utils.get_free_port()

    path3 = "peer3_identity/peer3_identity.txt"
    peer_id3 = identity.load_or_generate_peer_id(PEER_ID_FILE=path3)
    peer3address = Peer_utils.get_local_ip()
    peer3port = Peer_utils.get_free_port()
        
    process1 = multiprocessing.Process(
        target=peer1stuff,
        args=(peer_id1, peer1address, peer1port)
    )

    process2 = multiprocessing.Process(
        target=peer2stuff,
        args=(peer_id2, peer2address, peer2port)
    )

    process3 = multiprocessing.Process(
        target=peer3stuff,
        args=(peer_id3, peer3address, peer3port)
    )

    process2.start()
    time.sleep(5)  # Ensure peer2 starts before peer1
    process1.start()
    time.sleep(5)  # Ensure peer1 starts before peer3
    process3.start()



    process2.join()
    process1.join()
    process3.join()

    


    

    
    