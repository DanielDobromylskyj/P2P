import random
from abc import abstractmethod
from datetime import datetime
from statistics import median_high
from typing import Callable, TypedDict

# from threading import Lock

DEBUG: bool = True

if DEBUG:
    random.seed(1)  # For consistent testing

# Errors


class TooManyContactsError(Exception):
    """Raised when a contact is added to a full k-bucket."""
    pass


class OutOfRangeError(Exception):
    """Raised when a contact is added to a k-bucket that is out of range."""
    pass


class OurNodeCannotBeAContactError(Exception):
    """Raised when a contact added has the same ID as the client."""


class AllKBucketsAreEmptyError(Exception):
    """Raised when no KBuckets can be iterated through."""


class SendingQueryToSelfError(Exception):
    """Raised when a Query (RPC Call) is sent to ourselves."""
    pass


class SenderIsSelfError(Exception):
    """Raised when trying to send certain RPC commands, if sender is us."""
    pass


# class WithLock:
#     """
#     Lock object that can be used in "with" statements.
#     Example usage:
#         lock = threading.Lock()
#         with WithLock(lock):
#             do_stuff()
#         do_more_stuff()
#     Based from the following code:
#     https://www.bogotobogo.com/python/Multithread/python_multithreading_Synchronization_Lock_Objects_Acquire_Release.php
#     https://www.geeksforgeeks.org/with-statement-in-python/
#     """
#
#     def __init__(self, lock: Lock) -> None:
#         """
#         Creates lock object to be used in __enter__ and __exit__.
#         """
#         self.lock = lock
#
#     def __enter__(self) -> None:
#         """
#         Change the state to locked and returns immediately.
#         """
#         self.lock.acquire()
#
#     def __exit__(self, exc_type, exc_value, traceback) -> None:
#         """
#         Changes the state to unlocked; this is called from another thread.
#         """
#         self.lock.release()


class Constants:
    """
    https://xlattice.sourceforge.net/components/protocol/kademlia/specs.html

    A Kademlia network is characterized by three constants, which we call alpha, B, and k.
    The first and last are standard terms. The second is introduced because some Kademlia implementations use a
    different key length.

    alpha is a small number representing the degree of parallelism in network calls, usually 3
    B is the size in bits of the keys used to identify nodes and store and retrieve data; in basic Kademlia
    this is 160, the length of an SHA1 digest (hash)
    k is the maximum number of contacts stored in a bucket; this is normally 20
    It is also convenient to introduce several other constants not found in the original Kademlia papers.

    tExpire = 86400s, the time after which a key/value pair expires; this is a time-to-live (TTL) from the
    original publication date
    tRefresh = 3600s, after which an otherwise un-accessed bucket must be refreshed
    tReplicate = 3600s, the interval between Kademlia replication events, when a node is required to publish
    its entire database
    tRepublish = 86400s, the time after which the original publisher must republish a key/value pair
    """
    K: int = 20
    B: int = 160
    A: int = 3
    EXPIRATION_TIME_SEC: int = 86400  # TODO: Give this a proper number.


class ID:

    def __init__(self, value: int):
        """
        Kademlia node ID: This is an integer from 0 to 2^160 - 1

        Args:
            value: (int) ID denary value
        """

        self.MAX_ID = 2**160
        self.MIN_ID = 0
        if not (self.MAX_ID > value >= self.MIN_ID):
            # TODO: check if value >= self.MIN_ID is valid.
            raise ValueError(
                f"ID {value} is out of range - must a positive integer less than 2^160."
            )
        self.value = value

    def hex(self) -> str:
        return hex(self.value)

    def denary(self) -> int:
        return self.value

    def bin(self) -> str:
        """
        Returns value in binary - this does not include a 0b tag at the start.
        """
        return bin(self.value)[2:]

    def big_endian_bytes(self) -> list[str]:
        """
        Returns the ID in big-endian binary - largest bit is at index 0.
        """
        big_endian = [x for x in self.bin()[2:]]
        return big_endian

    def little_endian_bytes(self) -> list[str]:
        """
        Returns the ID in little-endian binary - smallest bit is at index 0.
        """
        big_endian = [x for x in self.bin()[2:]][::-1]
        return big_endian

    def __xor__(self, val) -> int:
        if type(val) == ID:
            return self.value ^ val.value
        return self.value ^ val

    def __eq__(self, val) -> bool:
        if type(val) == ID:
            return self.value == val.value
        return self.value == val

    def __ge__(self, val) -> bool:
        if type(val) == ID:
            return self.value >= val.value
        return self.value >= val

    def __le__(self, val) -> bool:
        if type(val) == ID:
            return self.value <= val.value
        return self.value <= val

    def __lt__(self, val) -> bool:
        if type(val) == ID:
            return self.value < val.value
        return self.value < val

    def __gt__(self, val) -> bool:
        if type(val) == ID:
            return self.value > val.value
        return self.value > val

    def __str__(self) -> str:
        return str(self.denary())

    @classmethod
    def max(cls) -> ID:
        """
        Returns max ID.
        :return: max ID.
        """
        return ID(2 ** 160 - 1)

    @classmethod
    def mid(cls) -> ID:
        """
        returns middle of the road ID
        :return: middle ID.
        """
        return ID(2 ** 159)

    @classmethod
    def min(cls) -> ID:
        """
        Returns minimum ID.
        :return: minimum ID.
        """
        return ID(0)

    @classmethod
    def random_id_within_bucket_range(cls, bucket) -> ID:
        """
        Returns an ID within the range of the bucket's low and high range.
        THIS IS NOT AN ID IN THE BUCKETS CONTACT LIST!
        (I mean it could be but shush)

        :param bucket: bucket to be searched
        :return: random ID in bucket.
        """
        return ID(bucket.low() + random.randint(0, bucket.high() - bucket.low()))

    @classmethod
    def random_id(cls, low=0, high=2 ** 160, seed=None) -> ID:
        """
        Generates a random ID, including both endpoints.

        FOR TESTING PURPOSES.
        Generating random ID's this way will not perfectly spread the prefixes,
        this is a maths law I've forgotten - due to the small scale of this
        I don't particularly see the need to perfectly randomise this.

        If I do though, here's how it would be done:
        - Randomly generate each individual bit, then concatenate.
        """
        if seed:
            random.seed(seed)
        return ID(random.randint(low, high))


class IStorage:
    """Interface which 'abstracts the storage mechanism for key-value pairs.''"""

    @abstractmethod
    def contains(self, key: ID) -> bool:
        pass

    @abstractmethod
    def try_get_value(self, key: ID) -> tuple[bool, int | str]:
        pass

    @abstractmethod
    def get(self, key: ID | int) -> str:
        pass

    @abstractmethod
    def get_timestamp(self, key: int) -> datetime:
        pass

    @abstractmethod
    def set(self, key: ID, value: str, expiration_time_sec: int = 0) -> None:
        pass

    @abstractmethod
    def get_expiration_time_sec(self, key: int) -> int:
        pass

    @abstractmethod
    def remove(self, key: int) -> None:
        pass

    @abstractmethod
    def get_keys(self) -> list[int]:
        pass

    @abstractmethod
    def touch(self, key: int) -> None:
        pass


class Contact:

    def __init__(self, id: ID, protocol=None):
        self.protocol: VirtualProtocol | IProtocol = protocol
        self.id = id
        self.last_seen: datetime = datetime.now()

    def touch(self) -> None:
        """Updates the last time the contact was seen."""
        self.last_seen = datetime.now()


class QueryReturn(TypedDict):
    """
    Has elements: contacts, val, found, found_by
    """
    contacts: list[Contact]
    val: str | None
    found: bool
    found_by: Contact | None


class Node:
    def __init__(self,
                 contact: Contact,
                 storage: IStorage,
                 cache_storage=None):

        self.our_contact: Contact = contact
        self._storage: IStorage = storage
        self.cache_storage = cache_storage
        self.DHT: DHT
        self.bucket_list = BucketList(contact.id)

    def ping(self, sender: Contact) -> Contact:
        # TODO: Complete.
        pass

    def store(self,
              key: ID,
              sender: Contact,
              val: str,
              is_cached: bool = False,
              expiration_time_sec: int = 0) -> None:

        if sender.id == self.our_contact.id:
            raise SenderIsSelfError("Sender should not be ourself.")

        # add sender to bucket_list (updating bucket list like how it is in spec.)
        self.bucket_list.add_contact(sender)

        if is_cached:
            self.cache_storage.set(key, val, expiration_time_sec)
        else:
            self.send_key_values_if_new_contact(sender)
            self._storage.set(key, val, Constants.EXPIRATION_TIME_SEC)

    def find_node(self, key: ID,
                  sender: Contact) -> tuple[list[Contact], str | None]:
        """
        Finds K close contacts to a given ID, while exluding the sender.
        It also adds the sender if it hasn't seen it before.
        :param key: K close contacts are found near this ID.
        :param sender: Contact to be excluded and added if new.
        :return: list of K (or less) contacts near the key
        """

        # managing sender
        if sender.id == self.our_contact.id:
            raise SendingQueryToSelfError("Sender cannot be ourselves.")
        self.send_key_values_if_new_contact(sender)
        self.bucket_list.add_contact(sender)

        # actually finding nodes
        # print([len(b.contacts) for b in self.bucket_list.buckets])
        contacts = self.bucket_list.get_close_contacts(key=key,
                                                       exclude=sender.id)
        # print(f"contacts: {contacts}")
        return contacts, None

    def find_value(self, key: ID, sender: Contact) \
            -> tuple[list[Contact] | None, str | None]:
        """
        Find value in self._storage, testing
        self.cache_storage if it is not in the former.
        If it is not in either, it gets K
        close contacts from the bucket list.
        """
        if sender.id == self.our_contact.id:
            raise SendingQueryToSelfError("Sender cannot be ourselves.")

        self.send_key_values_if_new_contact(sender)

        if self._storage.contains(key):
            return None, self._storage.get(key)
        elif self.cache_storage.contains(key):
            return None, self.cache_storage.get(key)
        else:
            return self.bucket_list.get_close_contacts(key, sender.id), None

    def send_key_values_if_new_contact(self, sender: Contact):
        # TODO: Complete this.
        pass

    def simply_store(self, key, val) -> None:
        """
        For unit testing.
        :param key:
        :param val:
        :return: None
        """
        self._storage.set(key, val)


class KBucket:
    def __init__(self,
                 initial_contacts: list[Contact] = None,
                 low: int = 0,
                 high: int = 2**160):
        """
        Initialises a k-bucket with a specific ID range, 
        initially from 0 to 2**160.
        """
        if initial_contacts is None:  # Fix for instead of setting initial_contacts = []
            initial_contacts = []

        self.contacts: list[Contact] = initial_contacts
        self._low = low
        self._high = high
        self.time_stamp: datetime = datetime.now()
        # self.lock = WithLock(Lock())

    def low(self) -> int:
        return self._low

    def high(self) -> int:
        return self._high

    def is_full(self) -> bool:
        """
        This INCLUDES K, so if there are 20 inside, no more can be added.
        :return: Boolean saying if it's full.
        """
        return len(self.contacts) >= Constants.K

    def contains(self, id: ID) -> bool:
        """
        Returns boolean determining whether a given contact ID is in the k-bucket.
        """

        # replaceable
        return any(id == contact.id for contact in self.contacts)

    def touch(self) -> None:
        self.time_stamp = datetime.now()

    def is_in_range(self, other_id: ID) -> bool:
        """
        Determines if a given ID is within the range of the k-bucket.
        :param other_id: The ID to be checked.
        :return: Boolean saying if it's in the range of the k-bucket.
        """
        return self._low <= other_id.value <= self._high

    def add_contact(self, contact: Contact) -> None:
        # TODO: Check if this is meant to check if it exists in the bucket.
        if self.is_full():
            raise TooManyContactsError(
                f"KBucket is full - (length is {len(self.contacts)}).")
        elif not self.is_in_range(contact.id):
            raise OutOfRangeError("Contact ID is out of range.")
        else:
            # !!! should this check if the contact is already in the bucket?
            self.contacts.append(contact)

    def depth(self) -> int:
        """
        "The depth is just the length of the prefix shared by all nodes in 
        the k-bucket’s range." Do not confuse that with this statement in the
        spec: “Define the depth, h, of a node to be 160 - i, where i is the 
        smallest index of a nonempty bucket.” The former is referring to the 
        depth of a k-bucket, the latter the depth of the node.
        """

        return len(self.shared_bits())

    def shared_bits(self) -> str:
        """
        Return the longest shared binary prefix between all 
        contacts in the kbucket. This does not "0b" before the binary.
        """

        def longest_shared_prefix_str(a: str, b: str) -> str:
            """Returns the longest common prefix between two strings."""

            if len(a) < len(b):  # swap "a" and "b" if "a" is shorter
                a, b = b, a

            for i in range(len(b)):
                if a[i] != b[i]:
                    return a[:i]

            return b

        longest_prefix = self.contacts[0].id.bin()[2:]  # first node id
        # print(self.contacts[0].id.bin()[2:], longest_prefix)
        for contact in self.contacts[1:]:
            id_bin = contact.id.bin()[2:]
            # print(id_bin)
            longest_prefix = longest_shared_prefix_str(id_bin, longest_prefix)
            # print(longest_prefix)

        return longest_prefix

    def split(self) -> tuple[KBucket, KBucket]:
        """
        Splits KBucket in half, returns tuple of type (KBucket, KBucket).
        """
        # This doesn't work when all contacts are bunched towards one side of the KBucket.
        # midpoint = (self._low + self._high) // 2

        # Gets the median of all contacts inside the KBucket (rounding up in even # of contacts)
        contact_ids_asc = sorted([c.id.value for c in self.contacts])
        median_of_contact_id: int = median_high(contact_ids_asc)
        midpoint = median_of_contact_id

        k1: KBucket = KBucket(low=self._low, high=midpoint)
        k2: KBucket = KBucket(low=midpoint, high=self._high)
        assert (len(k1.contacts) == 0)
        assert (len(k2.contacts) == 0)
        for c in self.contacts:
            if c.id.value < midpoint:
                k1.add_contact(c)
            else:
                k2.add_contact(c)

        return k1, k2

    def replace_contact(self, contact: Contact) -> None:
        """replaces contact, then touches it"""
        contact_ids = [c.id for c in self.contacts]
        index = contact_ids.index(contact.id)
        self.contacts[index] = contact
        contact.touch()


class BucketList:

    def __init__(self, our_id: ID):
        self.DHT = None
        self.buckets: list[KBucket] = [KBucket()]
        # first k-bucket has max range
        self.our_id: ID = our_id

        # create locking object
        # self.lock = WithLock(Lock())

    def can_split(self, kbucket: KBucket) -> bool:
        # kbucket.HasInRange(ourID) || ((kbucket.Depth() % Constants.B) != 0)
        """
        The depth to which the bucket has split is based on the number of bits
        shared in the prefix of the contacts in the bucket. With random IDs,
        this number will initially be small, but as bucket ranges become more
        narrow from subsequent splits, more contacts will begin the share the
        same prefix and the bucket when split, will result in less “room” for
        new contacts. Eventually, when the bucket range becomes narrow enough,
        the number of bits shared in the prefix of the contacts in the bucket
        reaches the threshold b, which the spec says should be 5.
        """
        # with self.lock:
        # TODO: What is self.node?

        return (kbucket.is_in_range(self.our_id)
                or (kbucket.depth() % Constants.B != 0))

    def _get_kbucket_index(self, other_id: ID) -> int:
        """
        Returns the first k-buckets index in the bucket list
        which has a given ID in range. Returns -1 if not found.
        """

        # with self.lock:
        for i in range(len(self.buckets)):
            if self.buckets[i].is_in_range(other_id):
                return i
        return -1

    def get_kbucket(self, other_id: ID) -> KBucket:
        """
        Returns the first k-bucket in the bucket list
        which has a given ID in range. Raises an error if none are found
         - this should never happen!
        :param other_id:  ID to used to determine range.
        :return: the first k-bucket which is in range.
        """

        try:
            bucket = self.buckets[self._get_kbucket_index(other_id)]
            return bucket

        except IndexError:
            raise OutOfRangeError(f"ID: {id} is not in range of bucket-list.")

    def add_contact(self, contact: Contact) -> None:
        """
        Adds a contact to a k-bucket in the list, this is determined by the range of k-buckets in the lists.
        This range should span the entire ID space - so there should always be a k-bucket to be added.

        This raises an error if we try to add ourselves to the k-bucket.

        :param contact: Contact to be added, this is touched in the process.
        :return: None
        """
        if self.our_id == contact.id:
            raise OurNodeCannotBeAContactError(
                "Cannot add ourselves as a contact.")

        contact.touch()  # Update the time last seen to now

        # with self.lock:
        kbucket: KBucket = self.get_kbucket(contact.id)
        if kbucket.contains(contact.id):
            print("Contact already in KBucket.")
            # replace contact, then touch it
            kbucket.replace_contact(contact)

        elif kbucket.is_full():
            if self.can_split(kbucket):
                # print("Splitting!")
                # Split then try again
                k1, k2 = kbucket.split()
                # print(f"K1: {len(k1.contacts)}, K2: {len(k2.contacts)}, Buckets: {self.buckets}")
                index: int = self._get_kbucket_index(contact.id)

                # adds the two buckets to 2 separate buckets.
                self.buckets[index] = k1  # Replaces original KBucket
                self.buckets.insert(index + 1, k2)  # Adds a new one after it
                # print(self.buckets)
                self.add_contact(contact)  # Unless k <= 0, This should never cause a recursive loop

            else:
                # TODO: Ping the oldest contact to see if it's still around and replace it if not.
                last_seen_contact: Contact = sorted(kbucket.contacts, key=lambda c: c.last_seen)[0]
                error: RPCError | None = last_seen_contact.protocol.ping(our_contact)
                if error:
                    if self.DHT:  # tests may not initialise a DHT
                        self.DHT.delay_eviction(last_seen_contact, contact)
                else:
                    # still can't add the contact ,so put it into the pending list
                    if self.DHT:
                        self.DHT.add_to_pending(contact)

        else:
            # Bucket is not full, nothing special happens.
            kbucket.add_contact(contact)

    def get_close_contacts(self, key: ID, exclude: ID) -> list[Contact]:
        """
        Brute force distance lookup of all known contacts, sorted by distance.
        Then we take K of the closest.
        :param key: The ID for which we want to find close contacts.
        :param exclude: The ID to exclude (the requesters ID).
        :return: List of K contacts sorted by distance.
        """
        # print(key, exclude)
        # with self.lock:
        contacts = []
        # print(self.buckets)
        for bucket in self.buckets:
            # print(bucket.contacts)
            for contact in bucket.contacts:
                # print(contact.id.value)
                # print(f"Exclude: {exclude}")

                if contact.id != exclude:
                    contacts.append(contact)
        # print(contacts)
        contacts = sorted(contacts, key=lambda c: c.id ^ key)[:Constants.K]
        if len(contacts) > Constants.K and DEBUG:
            raise ValueError(
                f"Contacts should be smaller than or equal to K. Has length {len(contacts)}, "
                f"which is {Constants.K - len(contacts)} too big.")
        return contacts

    def contacts(self) -> list[Contact]:
        contacts = []
        for bucket in self.buckets:
            for contact in bucket.contacts:
                contacts.append(contact)
        return contacts


class Router:
    """
    TODO: Talk about what this does.
    """

    def __init__(self, node: Node = None) -> None:
        """
        TODO: what is self.node?
        :param node:
        """
        self.node = node
        self.closer_contacts: list[Contact] = []
        self.further_contacts: list[Contact] = []
        # self.lock = WithLock(Lock())

    def lookup(self,
               key: ID,
               rpc_call: Callable,
               give_me_all: bool = False) -> QueryReturn:
        """
        Performs main Kademlia Lookup.
        :param key: Key to be looked up
        :param rpc_call: RPC call to be used.
        :param give_me_all: TODO: Implement.
        :return: returns query result.
        """
        have_work = True
        ret = []
        contacted_nodes = []
        # closer_uncontacted_nodes = []
        # further_uncontacted_nodes = []

        all_nodes = self.node.bucket_list.get_close_contacts(
            key, self.node.our_contact.id)[0:Constants.K]

        nodes_to_query: list[Contact] = all_nodes[0:Constants.A]

        for i in nodes_to_query:
            if i.id.value ^ key.value < self.node.our_contact.id.value ^ key.value:
                self.closer_contacts.append(i)
            else:
                self.further_contacts.append(i)

        # all untested contacts just get dumped here.
        for i in all_nodes[Constants.A + 1:]:
            self.further_contacts.append(i)

        for i in nodes_to_query:
            if i not in contacted_nodes:
                contacted_nodes.append(i)


        # In the spec they then send parallel async find_node RPC commands
        query_result: QueryReturn = (self.query(key, nodes_to_query, rpc_call,
                                                self.closer_contacts,
                                                self.further_contacts))

        if query_result["found"]:  # if a node responded
            return query_result

        # add any new closer contacts
        for i in self.closer_contacts:
            if i.id not in [j.id for j in ret]:  # if id does not already exist inside list
                ret.append(i)

        while len(ret) < Constants.K and have_work:
            closer_uncontacted_nodes = [
                i for i in self.closer_contacts if i not in contacted_nodes
            ]
            further_uncontacted_nodes = [
                i for i in self.further_contacts if i not in contacted_nodes
            ]

            # If we have uncontacted nodes, we still have work to be done.
            have_closer: bool = len(closer_uncontacted_nodes) > 0
            have_further: bool = len(further_uncontacted_nodes) > 0
            have_work = have_closer or have_further

            """
            Spec: of the k nodes the initiator has heard of closest to the target,
            it picks the 'a' that it has not yet queried and resends the FIND_NODE RPC
            to them.
            """
            if have_closer:
                new_nodes_to_query = closer_uncontacted_nodes[:Constants.A]
                for i in new_nodes_to_query:
                    if i not in contacted_nodes:
                        contacted_nodes.append(i)

                query_result = (self.query(key, new_nodes_to_query, rpc_call,
                                           self.closer_contacts,
                                           self.further_contacts))

                if query_result["found"]:
                    return query_result

            elif have_further:
                new_nodes_to_query = further_uncontacted_nodes[:Constants.A]
                for i in new_nodes_to_query:
                    if i not in contacted_nodes:
                        contacted_nodes.append(i)

                query_result = (self.query(key, new_nodes_to_query, rpc_call,
                                           self.closer_contacts,
                                           self.further_contacts))

                if query_result["found"]:
                    return query_result

        # return k closer nodes sorted by distance,

        contacts = sorted(ret[:Constants.K], key=(lambda x: x.id ^ key))
        return {
            "found": False,
            "contacts": contacts,
            "val": None,
            "found_by": None
        }

    def find_closest_nonempty_kbucket(self, key: ID) -> KBucket:
        """
        Helper method.
        Code listing 34.
        """
        # gets all non-empty buckets from bucket list
        non_empty_buckets: list[KBucket] = [
            b for b in self.node.bucket_list.buckets if (len(b.contacts) != 0)
        ]
        if len(non_empty_buckets) == 0:
            raise AllKBucketsAreEmptyError(
                "No non-empty buckets can be found.")

        return sorted(non_empty_buckets,
                      key=(lambda b: b.id.value ^ key.value))[0]

    @staticmethod
    def get_closest_nodes(key: ID, bucket: KBucket) -> list[Contact]:
        return sorted(bucket.contacts, key=lambda c: c.id.value ^ key.value)

    def rpc_find_nodes(self, key: ID, contact: Contact):
        # what is node??
        new_contacts, timeout_error = contact.protocol.find_node(
            self.node.our_contact, key)

        # dht?.handle_error(timeoutError, contact)

        return new_contacts, None, None

    def rpc_find_value(self, key, contact):
        # TODO: Create.
        pass

    def get_closer_nodes(self, key: ID, node_to_query: Contact, rpc_call,
                         closer_contacts: list[Contact],
                         further_contacts: list[Contact]) -> bool:

        contacts: list[Contact]
        found_by: Contact
        val: str
        contacts, found_by, val = rpc_call(key, node_to_query)
        peers_nodes = []
        for contact in contacts:
            if contact.id.value not in [
                    self.node.our_contact.id.value, node_to_query.id.value,
                    closer_contacts, further_contacts
            ]:
                peers_nodes.append(contact)

        nearest_node_distance = node_to_query.id.value ^ key.value

        # with self.lock:  # Lock thread while this is running.
        for p in peers_nodes:
            # if given nodes are closer than our nearest node
            # , and it hasn't already been added:
            if p.id.value ^ key.value < nearest_node_distance \
                    and p.id.value not in [i.id.value for i in closer_contacts]:
                closer_contacts.append(p)

        # with self.lock:  # Lock thread while this is running.
        for p in peers_nodes:
            # if given nodes are further than or equal to the nearest node
            # , and it hasn't already been added:
            if p.id.value ^ key.value >= nearest_node_distance \
                    and p.id.value not in [i.id.value for i in further_contacts]:
                further_contacts.append(p)

        return val is not None  # Can you use "is not" between empty strings and None?

    def query(self, key, new_nodes_to_query, rpc_call, closer_contacts,
              further_contacts) -> QueryReturn:
        pass


class IProtocol:
    # TODO: Create this!!
    # It shouldn't be too hard, it's just making skeletons
    # for type hinting all protocol methods.
    pass


class RPCError(Exception):
    """
    Errors for RPC commands.
    """

    @staticmethod
    def no_error():
        pass


class VirtualProtocol(IProtocol
                      ):
    """
    For unit testing, doesn't really do much
    """

    def __init__(self, node: Node | None = None, responds=True) -> None:
        self.responds = responds
        self.node = node

    def ping(self, sender: Contact) -> RPCError | None:
        if self.responds:
            self.node.ping(sender)
        else:
            return RPCError(
                "Time out while pinging contact - VirtualProtocol does not respond."
            )

    def find_node(self, sender: Contact,
                  key: ID) -> tuple[list[Contact], RPCError | None]:
        """
        Finds K close contacts to a given ID, while excluding the sender.
        It also adds the sender if it hasn't seen it before.
        :param key: K close contacts are found near this ID.
        :param sender: Contact to be excluded and added if new.
        :return: list of K (or less) contacts near the key, and an error that may need to be handled.
        """
        return self.node.find_node(sender=sender, key=key)[0], None

    def find_value(self, sender: Contact,
                   key: ID) -> tuple[list[Contact], str, RPCError | None]:
        """
        Returns either contacts or None if the value is found.
        """
        contacts, val = self.node.find_value(sender=sender, key=key)
        return contacts, val, None

    def store(self,
              sender: Contact,
              key: ID,
              val: str,
              is_cached=False,
              exp_time: int = 0) -> RPCError | None:
        """
        Stores the key-value on the remote peer.
        """
        self.node.store(sender=sender,
                        key=key,
                        val=val,
                        is_cached=is_cached,
                        expiration_time_sec=exp_time)

        return None


class VirtualStorage(IStorage):
    """
    Simple storage mechanism that stores things in memory.
    """

    def __init__(self):
        self._store: dict[int, str] = {}

    def contains(self, key: ID) -> bool:
        """
        Returns a boolean stating whether a key is storing something.
        """
        return key.value in list(self._store.keys())

    def get(self, key: ID | int) -> str:
        """
        Returns stored value, associated with given key value.
        :param key: Type ID or Integer, key value to be searched.
        :return:
        """
        if isinstance(type(key), ID):
            return self._store[key.value]
        elif isinstance(type(key), int):
            return self._store[key]
        else:
            raise TypeError("'get()' parameter 'key' must be type ID or int.")

    def get_timestamp(self, key: int) -> datetime:
        pass

    def set(self, key: ID, value: str, expiration_time_sec: int = 0) -> None:
        self._store[key.value] = value
        # TODO: Add expiration time to this.

    def get_expiration_time_sec(self, key: int) -> int:
        pass

    def remove(self, key: int) -> None:
        pass

    def get_keys(self) -> list[int]:
        pass

    def touch(self, key: int) -> None:
        pass

    def try_get_value(self, key: ID) -> tuple[bool, int | str]:
        pass


class DHT:
    """
    This is the main entry point for our peer to interact with other peers.

    This has multiple purposes:
     - One is to propagate key-values to other close peers on the network using a lookup algorithm.
     - Another is to use the same lookup algorithm to search for other close nodes that might have a value that we don’t have.
     - It is also used for bootstrapping our peer into a pre-existing network.

    """
    def __init__(self,
                 id: ID,
                 protocol: IProtocol,
                 storage_factory: Callable[[], IStorage],
                 router: Router):
        self._originator_storage = storage_factory()
        self.our_id = id
        self.our_contact = Contact(id=id, protocol=protocol)
        self.node = Node(self.our_contact, storage=VirtualStorage())
        self.node.DHT = self
        self.node.bucket_list.DHT = self
        self._protocol = protocol
        self._router: Router = router
        self._router.node = self.node
        self._router.DHT = self

    def router(self) -> Router:
        return self._router

    def protocol(self) -> IProtocol:
        return self._protocol

    def originator_storage(self) -> IStorage:
        return self._originator_storage

    def store(self, key: ID, val: str) -> None:
        self.touch_bucket_with_key(key)
        # We're storing to K closer contacts
        self._originator_storage.set(key, val)
        self.store_on_closer_contacts(key, val)

    def find_value(self, key: id) -> tuple[bool, list[Contact], str]:
        self.touch_bucket_with_key(key)
        contacts_queried: list[Contact] = []

        # ret (found: False, contacts: None, val: None)
        found: bool = False
        contacts: list[Contact] | None = None
        val: str | None = None

        # TODO: Talk about what this does - I haven't made it yet so IDK.
        our_val: str = self._originator_storage.try_get_value(key)[1]
        if our_val:
            found = True
            val = our_val
        else:
            lookup: QueryReturn = self._router.lookup(
                key, self._router.rpc_find_value)
            if lookup["found"]:
                found = True
                val = lookup["val"]
                # Find the closest contact (other than the one the value was found by)
                # in which to "cache" the key-value.
                close_contacts: list[Contact] = [
                    i for i in lookup["contacts"] if i != lookup["found_by"]
                ]

                if close_contacts:  # if a close contact exists.
                    store_to: Contact = sorted(close_contacts, key=lambda i: i.id ^ key)[0]
                    separating_nodes: int = self.get_separating_nodes_count(self.our_contact, store_to)
                    store_to.protocol.store(self.node.our_contact,
                                            key,
                                            lookup["val"],
                                            True,
                                            Constants.EXPIRATION_TIME_SEC)

        return found, contacts, val

    def touch_bucket_with_key(self, key: ID) -> None:
        pass

    def store_on_closer_contacts(self, key: ID, val: str):
        pass

    def bootstrap(self, known_peer: Contact) -> None:
        """
        This is how we join the network.

        We bootstrap our peer by contacting a known peer in the network, adding its contacts
        to our list, then getting the contacts for other peers not in the
        bucket range of our known peer we're joining.
        :param known_peer: Peer we know / are bootstrapping from.
        :return: RPC Error, not sure when it should be raised?
        """
        # print(f"Adding known peer with ID {known_peer.id}")
        self.node.bucket_list.add_contact(known_peer)

        # UNITTEST NOTES: This should return something in test_bootstrap_outside_bootstrapping_bucket,
        # it isn't at the moment.
        # find_node() should return the bucket list with the contact who knows 10 other contacts
        # it does.

        # finds K close contacts to self.our_id, excluding self.our_contact
        contacts, error = known_peer.protocol.find_node(sender=self.our_contact, key=self.our_id)
        # handle_error(error, known_peer)
        if not error:
            # print("NO ERROR")

            # add all contacts the known peer DIRECTLY knows
            for contact in contacts:
                self.node.bucket_list.add_contact(contact)

            known_peers_bucket: KBucket = self.node.bucket_list.get_kbucket(known_peer.id)

            if ID.max() in [c.id for c in known_peers_bucket.contacts]:
                print("somethings gone wrong")
            # Resolve the list now, so we don't include additional contacts
            # as we add to our bucket additional contacts.
            other_buckets: list[KBucket] = [i for i in self.node.bucket_list.buckets if i != known_peers_bucket]
            for other_bucket in other_buckets:
                self._refresh_bucket(other_bucket)  # UNITTEST Notes: one of these should contain the correct contact
        else:
            raise error

    def _refresh_bucket(self, bucket: KBucket) -> None:
        """
        Refreshes the given Kademlia KBucket by updating its last-touch timestamp,
        obtaining a random ID within the bucket's range, and attempting to find
        nodes in the network with that random ID.

        The method touches the bucket to update its last-touch timestamp, generates
        a random ID within the bucket's range, and queries nodes in the network
        using the Kademlia protocol to find nodes with the generated ID. If successful,
        the discovered contacts are added to the Kademlia node's bucket list.

        Note:
        The contacts collection for the given bucket might change during the operation,
        so it is isolated in a separate list before iterating over it.

        :param bucket: The KBucket to be refreshed.
        :returns: Nothing.
        """
        bucket.touch()
        random_id: ID = ID.random_id_within_bucket_range(bucket)

        # put in a separate list as contacts collection for this bucket might change.
        contacts: list[Contact] = bucket.contacts
        for contact in contacts:
            # print(contact.id, contact.protocol.node.bucket_list.contacts())
            if contact.id == ID.max():  # For unit testing, This should trigger!
                print("IMPORTANT")
            new_contacts, timeout_error = contact.protocol.find_node(
                self.our_contact, random_id)
            # print(contacts.index(contact) + 1, "new contacts", new_contacts)
            # handle_error(timeout_error, contact)
            if new_contacts:
                print("NEW CONTACTS")
                for other_contact in new_contacts:
                    self.node.bucket_list.add_contact(other_contact)


def empty_node() -> Node:
    """
    For testing.
    :return:
    """
    return Node(Contact(id=ID(0)), storage=VirtualStorage())


def random_node() -> Node:
    return Node(Contact(id=ID.random_id()), storage=VirtualStorage())


def select_random(arr: list, freq: int) -> list:
    return random.sample(arr, freq)
