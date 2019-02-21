# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Package for handling a list of objects of the same type as a given one."""

import threading
from Common import orb


class PeerList(object):

    """Class that builds a list of objects of the same type as this one."""

    def __init__(self, owner):
        self.owner = owner
        self.lock = threading.Condition()
        self.peers = {}

    # Public methods

    def initialize(self):
        """Populates the list of existing peers and registers the current
        peer at each of the discovered peers.

        It only adds the peers with lower ids than this one or else
        deadlocks may occur. This method must be called after the owner
        object has been registered with the name service.

        """

        self.lock.acquire()
        try:
            #
            # Your code here.
            #
            # Hämta alla reg peers(get_peers())
            # Registrera peer(owner) i deras lista om owner.id < peer.id
            # Lägg in dom i vår lista(self.peers) om peer.id < owner.id
            tmp_list = list()
            tmp_list = self.get_peers()
            print(self.get_peers())
            for peer in tmp_list:
                if owner.id < peer.id:
                    peer.peer_list.append(owner.id)
                elif peer.id < owner.id:
                    self.peers[peer.id] = peer

            print(tmp_list)
        finally:
            self.lock.release()

    def destroy(self):
        """Unregister this peer from all others in the list."""

        self.lock.acquire()
        try:
            #
            # Your code here.
            #
            # Hämta alla reg peers(get_peers())
            # Avregistrera peer(owner) i deras lista
            tmp_list = ()
            tmp_list = self.get_peers()
            for peer in tmp_list:
                for p in peer.peer_list:
                    if owner.id == p.id:
                        peer.peer_list.remove(p)
        finally:
            self.lock.release()

    def register_peer(self, pid, paddr):
        """Register a new peer joining the network."""

        # Synchronize access to the peer list as several peers might call
        # this method in parallel.
        self.lock.acquire()
        try:
            self.peers[pid] = orb.Stub(paddr)
            print("Peer {} has joined the system.".format(pid))
        finally:
            self.lock.release()

    def unregister_peer(self, pid):
        """Unregister a peer leaving the network."""
        # Synchronize access to the peer list as several peers might call
        # this method in parallel.

        self.lock.acquire()
        try:
            if pid in self.peers:
                del self.peers[pid]
                print("Peer {} has left the system.".format(pid))
            else:
                raise Exception("No peer with id: '{}'".format(pid))
        finally:
            self.lock.release()

    def display_peers(self):
        """Display all the peers in the list."""

        self.lock.acquire()
        try:
            pids = sorted(self.peers.keys())
            print("List of peers of type '{}':".format(self.owner.type))
            for pid in pids:
                addr = self.peers[pid].address
                print("    id: {:>2}, address: {}".format(pid, addr))
        finally:
            self.lock.release()

    def peer(self, pid):
        """Return the object with the given id."""

        self.lock.acquire()
        try:
            return self.peers[pid]
        finally:
            self.lock.release()

    def get_peers(self):
        """Return all registered objects."""

        self.lock.acquire()
        try:
            return self.peers
        finally:
            self.lock.release()
