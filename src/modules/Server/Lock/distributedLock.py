# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Module for the distributed mutual exclusion implementation.

This implementation is based on the second Ricart-Agrawala algorithm.
The implementation should satisfy the following requests:
    --  when starting, the peer with the smallest id in the peer list
        should get the token.
    --  access to the state of each peer (dictionaries: request, token,
        and peer_list) should be protected.
    --  the implementation should graciously handle situations when a
        peer dies unexpectedly. All exceptions coming from calling
        peers that have died, should be handled such as the rest of the
        peers in the system are still working. Whenever a peer has been
        detected as dead, the token, request, and peer_list
        dictionaries should be updated accordingly.
    --  when the peer that has the token (either TOKEN_PRESENT or
        TOKEN_HELD) quits, it should pass the token to some other peer.
    --  For simplicity, we shall not handle the case when the peer
        holding the token dies unexpectedly.
"""

NO_TOKEN = 0
TOKEN_PRESENT = 1
TOKEN_HELD = 2


class DistributedLock(object):

    """Implementation of distributed mutual exclusion for a list of peers.

    Public methods:
        --  __init__(owner, peer_list)
        --  initialize()
        --  destroy()
        --  register_peer(pid)
        --  unregister_peer(pid)
        --  acquire()
        --  release()
        --  request_token(time, pid)
        --  obtain_token(token)
        --  display_status()

    """

    def __init__(self, owner, peer_list):
        self.peer_list = peer_list
        self.owner = owner
        self.time = 0
        self.token = None
        self.request = {}
        self.state = NO_TOKEN
    def _prepare(self, token):
        """Prepare the token to be sent as a JSON message.

        This step is necessary because in the JSON standard, the key to
        a dictionary must be a string whild in the token the key is
        integer.
        """
        return list(token.items())

    def _unprepare(self, token):
        """The reverse operation to the one above."""
        return dict(token)

    # Public methods

    def initialize(self):
        """ Initialize the state, request, and token dicts of the lock.

        Since the state of the distributed lock is linked with the
        number of peers among which the lock is distributed, we can
        utilize the lock of peer_list to protect the state of the
        distributed lock (strongly suggested).

        NOTE: peer_list must already be populated when this
        function is called.

        """
        #
        # Your code here.
        #

        self.peer_list.lock.acquire()
        peers = self.peer_list.get_peers()
        min_peer = min(peers.keys())
        chosen_peer = self.peer_list.peer(min_peer)
        if (min_peer == self.owner.id):
            self.state = TOKEN_PRESENT
        self.token = { i : 0 for i in peers }
        self.request = { i : 0 for i in peers }

        self.peer_list.lock.release()

    def destroy(self):
        """ The object is being destroyed.

        If we have the token (TOKEN_PRESENT or TOKEN_HELD), we must
        give it to someone else.

        """
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        peers = self.peer_list.get_peers()
        the_bool_solution = False
        if self.state == TOKEN_PRESENT or self.state == TOKEN_HELD:
            if max(self.request.values()) > 0 and len(peers) > 1:
                tmp_list = []
                for key in peers:
                    tmp_list.append(key)
                owner_index = tmp_list.index(self.owner.id)
                i = owner_index + 1
                while True:
                    if i > (len(tmp_list)-1):
                        i = 0
                    pid = tmp_list[i]
                    if pid == self.owner.id:
                        break
                    if pid in self.request:
                        if self.request[pid] > self.token[pid]:
                            try:
                                tmp_peer = peers[pid]
                                send_token = self._prepare(self.token)
                                self.peer_list.lock.release()
                                tmp_peer.obtain_token(send_token)
                                self.peer_list.lock.acquire()
                                self.state = NO_TOKEN
                                break
                            except:
                                print("Selected peer could not obtain token...")
                                self.peer_list.lock.acquire()
                    i += 1
            else:
                for peer in peers:
                    try:
                        chosen_peer = peers[peer]
                        if self.owner.id == peer:
                            continue
                        else:
                            send_token = self._prepare(self.token)
                            self.peer_list.lock.release()
                            chosen_peer.obtain_token(send_token)
                            the_bool_solution = True
                    except:
                        print("Peer can not receive token, trying another peer")
                    finally:
                        self.peer_list.lock.acquire()
                        if the_bool_solution:
                            break
                self.state = NO_TOKEN
        self.peer_list.lock.release()

    def register_peer(self, pid):
        """Called when a new peer joins the system."""
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        self.token[pid] = self.time
        self.request[pid] = self.time
        self.peer_list.lock.release()


    def unregister_peer(self, pid):
        """Called when a peer leaves the system."""
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        try:
            del self.token[pid]
            del self.request[pid]
        except:
            print("Can't delete yo")
        finally:
            self.peer_list.lock.release()


    def acquire(self):
        """Called when this object tries to acquire the lock."""
        print("Trying to acquire the lock...")
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        self.time += 1
        peers = self.peer_list.get_peers()
        if self.state == NO_TOKEN:
            for peer in peers:
                try:
                    if peer != self.owner.id:
                        tmp_peer = peers[peer]
                        self.peer_list.lock.release()
                        tmp_peer.request_token(self.time, self.owner.id)
                        self.peer_list.lock.acquire()
                except:
                    print("Can not tell peer we want the token...")
                    self.peer_list.lock.acquire()

            self.peer_list.lock.release()
            while True:
                self.peer_list.lock.acquire()
                if self.state == TOKEN_PRESENT:
                    break
                self.peer_list.lock.release()

        self.state = TOKEN_HELD
        self.peer_list.lock.release()

    def release(self):
        """Called when this object releases the lock."""
        print("Releasing the lock...")
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        self.time += 1
        if self.state == TOKEN_PRESENT or self.state == TOKEN_HELD:
            self.token[self.owner.id] = self.time
            self.state = TOKEN_PRESENT
            peers = self.peer_list.get_peers()
            pid = self.owner.id + 1
            max_pid = max(peers.keys())
            sent_token = False
            self.peer_list.lock.release()
            while True:
                if pid > max_pid:
                    break
                if pid not in peers:
                    pid += 1
                    continue
                try:
                    self.peer_list.lock.acquire()
                    if self.request[pid] > self.token[pid]:
                        tmp_peer = peers[pid]
                        send_token = self._prepare(self.token)
                        self.peer_list.lock.release()
                        tmp_peer.obtain_token(send_token)
                        self.peer_list.lock.acquire()
                        self.state = NO_TOKEN
                        sent_token = True
                        break
                    else:
                        pid += 1
                except:
                    self.peer_list.lock.acquire()
                    pid += 1
                finally:
                    self.peer_list.lock.release()

            pid = min(peers.keys())
            self.peer_list.lock.acquire()
            while not sent_token:
                if pid == self.owner.id:
                    break
                if pid not in peers:
                    pid += 1
                    continue
                try:
                    if self.request[pid] > self.token[pid]:
                        tmp_peer = peers[pid]
                        send_token = self._prepare(self.token)
                        self.peer_list.lock.release()
                        tmp_peer.obtain_token(send_token)
                        self.peer_list.lock.acquire()
                        self.state = NO_TOKEN
                        break
                    else:
                        pid += 1
                except:
                    self.peer_list.lock.acquire()
                    pid += 1

        else:
            print("No token :(")
        self.peer_list.lock.release()

    def request_token(self, time, pid):
        """Called when some other object requests the token from us."""
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        try:
            self.time += 1
            if pid in self.request:
                self.request[pid] = max(self.request[pid], time)
            else:
                self.request[pid] = time
            if self.state == TOKEN_PRESENT:
                self.peer_list.lock.release()
                self.owner.release()
                self.peer_list.lock.acquire()
        except Exception as e:
            raise(e)
        finally:
            self.peer_list.lock.release()


    def obtain_token(self, token):
        """Called when some other object is giving us the token."""
        print("Receiving the token...")
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        self.time += 1
        token = self._unprepare(token)
        self.token = token
        self.state = TOKEN_PRESENT
        self.peer_list.lock.release()

    def display_status(self):
        """Print the status of this peer."""
        self.peer_list.lock.acquire()
        try:
            nt = self.state == NO_TOKEN
            tp = self.state == TOKEN_PRESENT
            th = self.state == TOKEN_HELD
            print("State   :: no token      : {0}".format(nt))
            print("           token present : {0}".format(tp))
            print("           token held    : {0}".format(th))
            print("Request :: {0}".format(self.request))
            print("Token   :: {0}".format(self.token))
            print("Time    :: {0}".format(self.time))
        finally:
            self.peer_list.lock.release()
