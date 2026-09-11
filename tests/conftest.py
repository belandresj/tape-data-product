"""Offline tests. Never access a sibling checkout or the network."""
import os
from pathlib import Path
import socket
import sys

ROOT = Path(__file__).resolve().parents[1]
_FORBIDDEN = (ROOT.parent / ('tape-' + 'characterization'), ROOT.parent / ('capitu' + 'lation'))

def _audit(event, args):
    if event == 'open' and isinstance(args[0], (str, bytes, os.PathLike)):
        path = Path(os.fsdecode(args[0])).resolve()
        if any(path == root or root in path.parents for root in _FORBIDDEN):
            raise RuntimeError('Test attempted to read a separate source checkout')
    if event in ('socket.connect', 'socket.getaddrinfo'):
        raise RuntimeError('Tests must remain offline; use a fake data client')

sys.addaudithook(_audit)
