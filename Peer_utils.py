"""Utility functions for network details used by peers.

This module provides simple helpers to fetch the local IP address, obtain a
free TCP port from the OS, and derive a broadcast address.
"""

import socket

def get_local_ip():
    """Return the local IP address.

    The function opens a UDP socket and connects to a public address
    (8.8.8.8:80) which doesn't need to be reachable; this allows the OS to
    choose the appropriate outbound interface and reveals the local IP in use.

    Returns:
        str: The local IPv4 address determined by the OS.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't need to be reachable
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def get_free_port():
    """Ask the OS for a free TCP port.

    Binds a temporary TCP socket to port 0, allowing the OS to select an
    available port. The selected port is returned and the socket is closed.

    Returns:
        int: An available TCP port number.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))   # 0 = ask OS for free port
    port = s.getsockname()[1]
    s.close()
    return port

def get_broadcast_address(ip):
    """Derive a broadcast address from an IPv4 address.

    Sets the last octet to 255. This is a simplistic approach sufficient for
    local development where a /24 subnet is assumed.

    Args:
        ip (str): The IPv4 address (e.g., "192.168.1.42").

    Returns:
        str: The broadcast address (e.g., "192.168.1.255").
    """
    parts = ip.split('.')
    parts[-1] = '255'
    return '.'.join(parts)

# print("Local IP Address:", get_local_ip())
# print("Free Port:", get_free_port())