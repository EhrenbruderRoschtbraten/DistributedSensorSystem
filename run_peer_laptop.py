import argparse

from Peer import Peer
from Peer_utils import get_free_port, get_local_ip


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Start a peer for multi-laptop testing."
    )
    parser.add_argument(
        "--address",
        default=None,
        help="Local IP address to bind for this peer (defaults to auto-detect).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="TCP port for this peer (defaults to a free OS-selected port).",
    )
    args = parser.parse_args()

    address = args.address or get_local_ip()
    port = args.port or get_free_port()
    peer = Peer(address=address, port=port)
    peer.start()


if __name__ == "__main__":
    main()
