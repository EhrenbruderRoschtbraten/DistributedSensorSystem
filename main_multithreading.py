"""Launch multiple peers in separate processes for local testing.

This script starts three peers with auto-generated UUIDs and staggers their
startup to reduce contention.
"""

import Peer
import multiprocessing
import socket
import time
import Peer_utils


#initialize two peers
#start peer1 listening socket in a separate process
def peer1stuff(peer1address, peer1port):
    """Run a peer instance in the current process.

    Args:
        peer1address (str): Local IP address for the peer.
        peer1port (int): TCP port for the peer's server socket.
    """
    peer1 = Peer.Peer(address=peer1address, port=peer1port)
    peer1.start()


        



#start peer2 listening socket in a separate process
def peer2stuff(peer2address, peer2port):
    """Run a second peer instance in the current process.

    Args:
        peer2address (str): Local IP address for the peer.
        peer2port (int): TCP port for the peer's server socket.
    """
    peer2 = Peer.Peer(address=peer2address, port=peer2port)
    peer2.start()

#start peer2 listening socket in a separate process
def peer3stuff(peer3address, peer3port):
    """Run a third peer instance in the current process.

    Args:
        peer3address (str): Local IP address for the peer.
        peer3port (int): TCP port for the peer's server socket.
    """
    peer3 = Peer.Peer(address=peer3address, port=peer3port)
    peer3.start()


if __name__ == '__main__':

    peer1address = Peer_utils.get_local_ip()
    peer1port = Peer_utils.get_free_port()
     
    peer2address = Peer_utils.get_local_ip()
    peer2port = Peer_utils.get_free_port()

    peer3address = Peer_utils.get_local_ip()
    peer3port = Peer_utils.get_free_port()
        
    process1 = multiprocessing.Process(
        target=peer1stuff,
        args=(peer1address, peer1port)
    )
    process1.daemon = True

    process2 = multiprocessing.Process(
        target=peer2stuff,
        args=(peer2address, peer2port)
    )
    process2.daemon = True

    process3 = multiprocessing.Process(
        target=peer3stuff,
        args=(peer3address, peer3port)
    )
    process3.daemon = True

    process2.start()
    time.sleep(5)  # Ensure peer2 starts before peer1
    process1.start()
    time.sleep(5)  # Ensure peer1 starts before peer3
    process3.start()



    process2.join()
    process1.join()
    process3.join()

    


    

    
    