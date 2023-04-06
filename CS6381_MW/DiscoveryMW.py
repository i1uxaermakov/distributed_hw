###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.

import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import json # for reading the dht.json file
import uuid # for creating unique identity strings
import hashlib  # for the secure hash library

from CS6381_MW import discovery_pb2


# A class that defines a data structure used for finger table
class FingerTableEntry():
  def __init__(self, hash, corresponding_node_info):
    self.hash = hash
    self.node_info = corresponding_node_info
    self.dealer_socket = None


class DiscoveryMW ():
  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.logger = logger  # internal logger for print statements
    self.router = None # will be a ZMQ router socket to receive requests and respond to them
    self.poller = None # used to wait on incoming replies
    self.upcall_obj = None # handle to appln obj to handle appln-specific data
    self.handle_events = True # in general we keep going thru the event loop
    self.port = None
    self.finger_table = [] # finger table for DHT ring
    self.dht_json_path = None
    self.my_dht_hash = None

    # Zookeeper-related fields
    self.sync_pub_socket = None # Socket for publishing updates in case you are a leader
    self.sync_pub_port = None # Port for the pub socket
    self.sync_sub_socket = None # Socket for subscribing to updates from the leader discovery
    


  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.info ("DiscoveryMW::configure")
      
      # Set the port to bind to
      self.port = args.port

      # Next get the ZMQ context
      self.logger.debug ("DiscoveryMW::configure - obtain ZMQ context")
      context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("DiscoveryMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the ROUTER
      self.logger.debug ("DiscoveryMW::configure - obtain ROUTER socket")
      self.router = context.socket (zmq.ROUTER)

      # Listen for incoming requests on Router Socket
      self.logger.debug ("DiscoveryMW::configure - register the ROUTER socket for incoming replies")
      self.poller.register (self.router, zmq.POLLIN)

      # Set up dht-related arguments
      self.dht_json_path = args.dht_json_path
      self.name = args.name

      # If using DHT Lookup
      if(self.upcall_obj.lookup == "DHT"):
        # Set up the finger table
        # Set up the table entries
        self.set_up_finger_table()
        
        # Set up a socket for each entry
        for entry in self.finger_table:
          # Create Socket
          entry.dealer_socket = context.socket(zmq.DEALER)
          
          # Bind the socket to the address of the corresponding node
          dealer_bind_str = "tcp://" + entry.node_info['IP'] + ":" + str(entry.node_info['port'])
          entry.dealer_socket.connect(dealer_bind_str)

          # Set identity of the socket
          dealer_uuid = bytes(uuid.uuid4().hex, 'utf-8')
          entry.dealer_socket.setsockopt(zmq.IDENTITY, dealer_uuid)

          # register the dealer socket with poller
          self.poller.register (entry.dealer_socket, zmq.POLLIN)

      # If using ZooKeeper lookup
      elif(self.upcall_obj.lookup == 'ZooKeeper'):
        # set up sockets
        self.sync_pub_port = args.sub_port
        self.sync_pub_socket = context.socket(zmq.PUB)
        bind_string = "tcp://*:" + str(self.sync_pub_port)
        self.sync_pub_socket.bind (bind_string)
        
        self.sync_sub_socket = context.socket(zmq.SUB)
      
      # Now bind to the socket for incoming requests. We are ready to accept requests from anyone, so the string is tcp://*:*
      self.logger.debug ("DiscoveryMW::configure - bind to the socket and port")
      bind_str = "tcp://*:" + str(self.port)
      self.router.bind (bind_str)
      
      self.logger.debug ("DiscoveryMW::configure completed")

    except Exception as e:
      raise e

  ########################################
  # set_up_finger_table
  ########################################
  def set_up_finger_table(self):
    f = open(self.dht_json_path)
    dht_file = json.load(f) # get dht.hson as a dictionary

    # Sort DHT nodes by hash
    dht_file['dht'] = sorted(dht_file['dht'], key=lambda d: d['hash']) 

    # Find yourself in the dht file and get the hash
    for dht_info in dht_file['dht']:
      if(dht_info['id'] == self.name):
        self.my_dht_hash = dht_info['hash']
        break

    # Populate the finger table
    address_space = (2 ** 48)
    for i in range(0, 48):
      new_hash = (self.my_dht_hash + (2 ** i)) % address_space
      
      # go over all dhts and find the smallest entry that is largest or equal to new hash
      successor = {
          'id': '',
          'hash': address_space,
          'IP': '',
          'port': 0,
          'host': ''
      }
      for dht_node in dht_file['dht']:
          if(dht_node['hash'] >= new_hash and successor['hash'] > dht_node['hash']):
              successor = dht_node
      
      # if we haven't found a hash that is larger, then we assign 
      # it to the first node in the ring
      if(successor['hash'] == address_space):
          successor = dht_file['dht'][0]
          
      self.finger_table.append(FingerTableEntry(successor['hash'], successor))
    
    # for entry in self.finger_table:
    #   self.logger.info(str([entry.hash, entry.node_info]))




  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################
  def event_loop (self, timeout=None):

    try:
      self.logger.info ("DiscoveryMW::event_loop - run the event loop")

      # we are using a class variable called "handle_events" which is set to
      # True but can be set out of band to False in order to exit this forever
      # loop
      while self.handle_events:  # it starts with a True value
        # poll for events. We give it an infinite timeout.
        # The return value is a socket to event mask mapping
        events = dict (self.poller.poll (timeout=timeout))
        
        request_handled = False

        # check if a timeout has occurred. We know this is the case when
        # the event mask is empty
        if not events:
          # we are ready to shut down because everybody has already registered
          timeout = self.upcall_obj.stop_appln()
          request_handled = True
          
        if (not request_handled) and (self.router in events):
          # handle the incoming request
          timeout = self.handle_request ()
          request_handled = True

        if (not request_handled) and (self.sync_sub_socket in events):
          # handle a sync request from leader discovery
          timeout = self.handle_state_sync_request ()
          request_handled = True

        if (self.upcall_obj.lookup == "DHT") and (not request_handled):
          # check all dealer sockets in case we are using DHT ring
          for entry in self.finger_table:
            # if we receive something on a dealer socket, it is a response to q request we sent earlier
            # a Discovery node is never an originator of that request, so we send it back using router. It will go back to either another Discovery node
            if entry.dealer_socket in events:
              message = entry.dealer_socket.recv_multipart()
              self.router.send_multipart(message)
              request_handled = True
              break
          
        if not request_handled:
          raise Exception ("Unknown event after poll")

      self.logger.info ("DiscoveryMW::event_loop - out of the event loop")
    except Exception as e:
      raise e


  #################################################################
  # handle an incoming request
  #################################################################
  def handle_request (self):

    try:
      self.logger.debug ("DiscoveryMW::handle_request")

      # Receive all frames
      framesRcvd = self.router.recv_multipart()
      bytesRcvd = framesRcvd[-1]
      self.logger.debug ("DiscoveryMW::handle_request – received bytes and frames")
      self.logger.debug (f"DiscoveryMW::handle_request – frames received: {framesRcvd}")

      # now use protobuf to deserialize the bytes
      # The way to do this is to first allocate the space for the
      # message we expect, here DiscoveryResp and then parse
      # the incoming bytes and populate this structure (via protobuf code)
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.ParseFromString (bytesRcvd)

      # demultiplex the message based on the message type but let the application
      # object handle the contents as it is best positioned to do so.
      # Note also that we expect the return value to be the desired timeout to use
      # in the next iteration of the poll.
      if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
        self.logger.debug (f"DiscoveryMW::handle_request – Received a register request: do_read_or_write {disc_req.do_read_or_write}, from {disc_req.register_req.info.id}")

        if(self.upcall_obj.lookup == "DHT"):
          self.logger.debug ("DiscoveryMW::handle_request – Processing a DHT Register request")
          # If this node is the one responsible for the current hash, save data and respond to request
          if (disc_req.do_read_or_write):
            self.logger.debug ("DiscoveryMW::handle_request – We are the node responsible for the hash")
            # let the appln level object decide what to do
            self.logger.debug ("DiscoveryMW::handle_request – sending the REGISTER request to be handled in the upcall object because we are the DHT node that needs to handle it")
            timeout = self.upcall_obj.handle_register_request (disc_req.register_req, framesRcvd, disc_req.timestamp_sent)
          else:
            # If the current node is not the one holding the hash, compute the next node to forward request to and send the message there

            self.logger.debug ("DiscoveryMW::handle_request – The node responsible for hash hasn't been identified, so we try to find it")

            # Compute hash
            entity_hash = self.compute_hash_for_registring_entity(disc_req.register_req)

            # Find what node to forward the request to based on the hash
            node, found_the_one = self.find_successor(entity_hash)
            self.logger.debug (f"DiscoveryMW::handle_request – found_the_one={found_the_one}")

            # Construct the message
            new_disc_req = self.create_register_req_to_next_dht_node(disc_req, found_the_one)
            buf2send = new_disc_req.SerializeToString ()

            self.logger.debug (f"DiscoveryMW::handle_request – Forwarding the request from myself (node {self.upcall_obj.name}) to node: {node.node_info['id']}, do_read_or_write={new_disc_req.do_read_or_write}")

            # Update the message in the frames
            framesRcvd[-1] = buf2send

            # Send the message to the node
            node.dealer_socket.send_multipart(framesRcvd)

            timeout = None
        
        # NOT using DHT lookup
        else: 
          # let the appln level object decide what to do
          self.logger.debug ("DiscoveryMW::handle_request – sending the REGISTER request to be handled in the upcall object")
          timeout = self.upcall_obj.handle_register_request (disc_req.register_req, framesRcvd, disc_req.timestamp_sent)
      
      elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
        # this is a response to is ready request
        self.logger.debug ("DiscoveryMW::handle_request – sending the ISREADY request to be handled in the upcall object")
        timeout = self.upcall_obj.handle_isready_request (disc_req.isready_req, framesRcvd, disc_req.timestamp_sent)
      
      elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # received a lookup request
        self.logger.debug ("DiscoveryMW::handle_request – sending the LOOKUP PUBLISHERS BY TOPICS request to be handled in the upcall object")
        timeout = self.upcall_obj.handle_lookup_pub_by_topics(disc_req.lookup_req, False, framesRcvd, disc_req.timestamp_sent)

      elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
        # received a lookup request
        self.logger.debug ("DiscoveryMW::handle_request – sending the LOOKUP ALL PUBLISHERS request to be handled in the upcall object")
        timeout = self.upcall_obj.handle_lookup_pub_by_topics(disc_req.lookup_req, True, framesRcvd, disc_req.timestamp_sent)

      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise ValueError ("Unrecognized response message")

      return timeout
    
    except Exception as e:
      raise e


  ########################################
  # forward_isready_request_further
  #
  # Propagate the is ready request further to 
  # collect data about registered entities
  ########################################
  def forward_isready_request_further(self, visited_nodes_set, registered_pubs_set, registered_subs_set, registered_brokers_set, framesRcvd, timestamp_sent):
    # Form a message and send it to the first entry in the finger table
    dht_payload = discovery_pb2.DhtIsReadyPayload () # allocate
    dht_payload.visited_nodes[:] = list(visited_nodes_set)
    dht_payload.registered_subs[:] = list(registered_subs_set)
    dht_payload.registered_pubs[:] = list(registered_pubs_set)
    dht_payload.registered_brokers[:] = list(registered_brokers_set)

    isready_req = discovery_pb2.IsReadyReq ()
    isready_req.dht_payload.CopyFrom(dht_payload)

    disc_req = discovery_pb2.DiscoveryReq ()
    disc_req.msg_type = discovery_pb2.TYPE_ISREADY
    disc_req.isready_req.CopyFrom (isready_req)
    disc_req.timestamp_sent = timestamp_sent
    
    buf2send = disc_req.SerializeToString ()
    
    # Update the message in the frames
    framesRcvd[-1] = buf2send

    # Send the message to the immediate successor in finger table
    self.finger_table[0].dealer_socket.send_multipart(framesRcvd)
    return
  
  ########################################
  # forward_lookup_request_further
  #
  # Propagate the lookup request further to 
  # collect data about registered entities
  ########################################
  def forward_lookup_request_further(self, visited_nodes_set, already_added_sockets, topiclist, all, framesRcvd, timestamp_sent):
    lookup_req = discovery_pb2.LookupPubByTopicReq ()
    lookup_req.topiclist[:] = topiclist
    lookup_req.visited_nodes[:] = list(visited_nodes_set)
    lookup_req.sockets_to_connect_to[:] = list(already_added_sockets)

    disc_req = discovery_pb2.DiscoveryReq ()
    if(all):
      disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
    else:
      disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
    disc_req.lookup_req.CopyFrom (lookup_req)
    disc_req.timestamp_sent = timestamp_sent
    
    buf2send = disc_req.SerializeToString ()
    
    # Update the message in the frames
    framesRcvd[-1] = buf2send

    # Send the message to the immediate successor in finger table
    self.finger_table[0].dealer_socket.send_multipart(framesRcvd)
    return


  ########################################
  # create_register_req_to_next_dht_node
  #
  # Create register request to the next dht node
  ########################################
  def create_register_req_to_next_dht_node(self, rcvd_register_req, found_the_one):
    # attributes = dir(rcvd_disc_req)
    # for attribute in attributes:
    #     self.logger.info(attribute)

    reg_info = discovery_pb2.RegistrantInfo () # allocate
    reg_info.id = rcvd_register_req.register_req.info.id
    reg_info.addr = rcvd_register_req.register_req.info.addr
    reg_info.port = rcvd_register_req.register_req.info.port

    register_req = discovery_pb2.RegisterReq ()
    register_req.role = rcvd_register_req.register_req.role
    register_req.info.CopyFrom (reg_info)
    register_req.topiclist[:] = rcvd_register_req.register_req.topiclist

    disc_req = discovery_pb2.DiscoveryReq ()
    disc_req.msg_type = discovery_pb2.TYPE_REGISTER
    disc_req.register_req.CopyFrom (register_req)
    disc_req.do_read_or_write = found_the_one
    disc_req.timestamp_sent = rcvd_register_req.timestamp_sent # statistics

    self.logger.debug(f"Original Message: {rcvd_register_req.register_req.info.id}, {rcvd_register_req.register_req.info.addr}, {rcvd_register_req.register_req.info.port}, {rcvd_register_req.register_req.role}, {rcvd_register_req.register_req.topiclist}, {rcvd_register_req.do_read_or_write}")

    self.logger.debug(f"New Message: {disc_req.register_req.info.id}, {disc_req.register_req.info.addr}, {disc_req.register_req.info.port}, {disc_req.register_req.role}, {disc_req.register_req.topiclist}, {disc_req.do_read_or_write}")

    return disc_req



  ########################################
  # compute_hash_for_registring_entity
  #
  # Compute hash that will represent a broker, subscriber, or publisher
  ########################################
  def compute_hash_for_registring_entity(self, register_req):
    string_to_hash = ''
    if(register_req.role == discovery_pb2.ROLE_SUBSCRIBER):
      string_to_hash = register_req.info.id
    else: # broker, publisher
      string_to_hash = register_req.info.id + ":" + register_req.info.addr + ":" + str(register_req.info.port)

    self.logger.info(f"compute_hash_for_registring_entity: string {string_to_hash}")
    self.logger.info(f"compute_hash_for_registring_entity: hash {self.hash_func(string_to_hash)}")
    return self.hash_func(string_to_hash)

  #################
  # hash value
  #################
  def hash_func (self, id):
    # first get the digest from hashlib and then take the desired number of bytes from the
    # lower end of the 256 bits hash. Big or little endian does not matter.
    hash_digest = hashlib.sha256(bytes(id, "utf-8")).digest()  # this is how we get the digest or hash value
    # figure out how many bytes to retrieve
    num_bytes = int(48/8)  # otherwise we get float which we cannot use below
    hash_val = int.from_bytes (hash_digest[:num_bytes], "big")  # take lower N number of bytes
    return hash_val
  
  #################
  # find_successor
  #
  # Returns finger table entry to query next and True if that node is the one that is responsible for the hash and false otherwise
  #################
  def find_successor(self, hash_searched):
    self.logger.info(f"My Hash: {self.my_dht_hash}, {type(self.my_dht_hash)}")
    self.logger.info(f"Searched Hash: {hash_searched}, {type(hash_searched)}")
    self.logger.info(f"Successor Hash: {self.finger_table[0].hash}, {type(self.my_dht_hash)}")

    successor_in_finger_table = self.finger_table[0]
    if(hash_searched > self.my_dht_hash and hash_searched <= successor_in_finger_table.hash):
        self.logger.info (f"find_successor: FOUND THE ONE")
        return successor_in_finger_table, True
    elif(successor_in_finger_table.hash < self.my_dht_hash and (hash_searched > self.my_dht_hash or hash_searched < successor_in_finger_table.hash)):
        self.logger.info (f"find_successor: FOUND THE ONE")
        return successor_in_finger_table, True
    else:
        self.logger.info (f"find_successor: MAKING REQUEST TO ANOTHER ONE")
        n_dot = self.find_closest_preceding_node(hash_searched)
        return n_dot, False
    

  #################
  # find_closest_preceding_node
  #
  # Returns finger table entry to query next and True if that node is the one that is responsible for the hash and false otherwise
  #################
  def find_closest_preceding_node(self, hash_searched):
    for entry in reversed(self.finger_table):
      if(self.my_dht_hash >= hash_searched):
        if(entry.hash > self.my_dht_hash or entry.hash < hash_searched):
          return entry
      else:
        if(entry.hash > self.my_dht_hash and entry.hash < hash_searched):
          return entry
    raise ValueError ("Reached the point we should not have reached – no appropriate node was found in the finger table")
    
  
  ########################################
  # respond_to_register_request
  #
  # Respond to register request. was_successful indicates whether the 
  # request for registration was successful. If it wasn't, reason variable 
  # is going to have the reason for failure
  ########################################
  def respond_to_register_request(self, framesRcvd, was_successful, reason, timestamp_sent):
    ''' respond_to_register_request '''

    try:
      self.logger.debug ("DiscoveryMW::respond_to_register_request")
    
      # Create the payload for register response
      register_response = discovery_pb2.RegisterResp()
      if(was_successful):
        register_response.status = discovery_pb2.STATUS_SUCCESS
      else:
        register_response.status = discovery_pb2.STATUS_FAILURE
        register_response.reason = reason

      # Finally, build the outer layer DiscoveryResp Message
      disc_resp = discovery_pb2.DiscoveryResp ()  # allocate
      disc_resp.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.register_resp.CopyFrom (register_response)
      disc_resp.timestamp_sent = timestamp_sent # statistics
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to the service that sent the request
      self.logger.debug ("DiscoveryMW::respond_to_register_request - send isready stringified buffer to service that sent the request")
      
      # Update the message in the frames
      framesRcvd[-1] = buf2send

      # Send the message to the node
      self.router.send_multipart(framesRcvd)
      
    
    except Exception as e:
      raise e
    

  ########################################
  # respond_to_isready_request
  #
  ########################################
  def respond_to_isready_request(self, is_system_ready, framesRcvd, timestamp_sent):
    ''' respond_to_isready_request '''

    try:
      self.logger.debug ("DiscoveryMW::respond_to_isready_request")
    
      # Create the payload for register response
      isready_response = discovery_pb2.IsReadyResp()
      isready_response.status = is_system_ready

      # Finally, build the outer layer DiscoveryResp Message
      disc_resp = discovery_pb2.DiscoveryResp ()  # allocate
      disc_resp.msg_type = discovery_pb2.TYPE_ISREADY  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.isready_resp.CopyFrom (isready_response)
      disc_resp.timestamp_sent = timestamp_sent # statistics
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # Update the message in the frames
      framesRcvd[-1] = buf2send

      # now send this to the service that sent the request
      self.logger.debug ("DiscoveryMW::respond_to_isready_request - send isready stringified buffer to the service that requested")
      self.router.send_multipart(framesRcvd)
    
    except Exception as e:
      raise e




  ########################################
  # respond_to_lookup_request
  ########################################
  def respond_to_lookup_request(self, publisher_ipports, all, framesRcvd, timestamp_sent):
    ''' respond_to_lookup_request '''

    try:
      self.logger.debug ("DiscoveryMW::respond_to_lookup_request")
    
      # Create the payload for register response
      lookup_response = discovery_pb2.LookupPubByTopicResp()
      lookup_response.addressesToConnectTo[:] = list(publisher_ipports)

      # Finally, build the outer layer DiscoveryResp Message
      disc_resp = discovery_pb2.DiscoveryResp ()  # allocate
      if(all):
        disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS  # set message type
      else:
        disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # set message type

      disc_resp.lookup_resp.CopyFrom (lookup_response)
      disc_resp.timestamp_sent = timestamp_sent # statistics
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # Update the message in the frames
      framesRcvd[-1] = buf2send

      # now send this to the service that sent the request
      self.logger.debug ("DiscoveryMW::respond_to_lookup_request - send lookup_response stringified buffer to the service that requested")
      self.router.send_multipart (framesRcvd)  # we use the "send" method of ZMQ that sends the bytes
    
    except Exception as e:
      raise e




  ########################################
  # set upcall handle
  #
  # here we save a pointer (handle) to the application object
  ########################################
  def set_upcall_handle (self, upcall_obj):
    ''' set upcall handle '''
    self.upcall_obj = upcall_obj




  ########################################
  # disable event loop
  #
  # here we just make the variable go false so that when the event loop
  # is running, the while condition will fail and the event loop will terminate.
  ########################################
  def disable_event_loop (self):
    ''' disable event loop '''
    self.handle_events = False



  ########################################
  # subscribe_for_updates_from_leader
  #
  ########################################
  def subscribe_for_updates_from_leader(self, leader_addr, leader_sub_port):
    # Connect to the leader
    self.logger.info(f'MW: Connecting to {"tcp://" + leader_addr + str(leader_sub_port)}')
    self.sync_sub_socket.connect ("tcp://" + leader_addr + ':' + str(leader_sub_port))
    
    # Subscribe to discovery updates
    self.sync_sub_socket.setsockopt (zmq.SUBSCRIBE, bytes('discovery', 'utf-8'))

    # Register with poller
    self.poller.register (self.sync_sub_socket, zmq.POLLIN)
    return
  

  ########################################
  # publish_discovery_update
  # 
  # Used when a new entity (publisher) registers, tells other discoveries to sync state
  ########################################
  def publish_discovery_update(self):
    # We send all of the state we have
    state = {
      'registered_publishers': list(self.upcall_obj.registered_publishers),
      'publisher_id_to_ipport_mapping': self.upcall_obj.publisher_id_to_ipport_mapping,
      'topic_to_publishers_id_mapping': self.upcall_obj.topic_to_publishers_id_mapping,
      'registered_subscribers': list(self.upcall_obj.registered_subscribers),
      'registered_brokers': list(self.upcall_obj.registered_brokers),
      'broker_id_to_ipport_mapping': self.upcall_obj.broker_id_to_ipport_mapping
    }
    state_bytes = json.dumps(state).encode('utf-8')

    self.logger.info(f"Publishing a DISCOVERY SYNC update: {str(state)}")

    # Send from the socket
    send_str = b'discovery:' + state_bytes
    self.sync_pub_socket.send (send_str)

    return

  ########################################
  # publish_unsub_update
  # 
  # Used when a publisher or broker dies
  ########################################
  def publish_unsub_update(self, unsub_update_body):
    self.logger.info(f"Publishing a UNSUB update: {str(unsub_update_body)}")
    unsub_update_bytes = json.dumps(unsub_update_body).encode('utf-8')
    
    # Send from the socket
    send_str = b'unsub:' + unsub_update_bytes
    self.sync_pub_socket.send (send_str)
    return
  
  ########################################
  # publish_sub_update – update to subscription (i.e. update who you are subscribed to)
  # 
  # Used when a publisher has registered or a new broker leader has been elected. Tells brokers and subscribers to subscribe to it
  ########################################
  def publish_sub_update(self, sub_update_body):
    self.logger.info(f"Publishing a SUB update: {str(sub_update_body)}")
    sub_update_bytes = json.dumps(sub_update_body).encode('utf-8')
    
    # Send from the socket
    send_str = b'sub:' + sub_update_bytes
    self.sync_pub_socket.send (send_str)
    return
  
  ########################################
  # handle_state_sync_request
  # 
  # Handle an update to the state. A primary Discovery service has sent us a request, so we update our state
  ########################################  
  def handle_state_sync_request(self):
    self.logger.info("Handling a state sync request")

    # Get the message from SUB socket
    # Receive all frames
    bytesRcvd = self.sync_sub_socket.recv()
    bytesRcvd = bytesRcvd.decode('utf-8')
    beginning_of_payload = (bytesRcvd.find(':') + 1)
    string_received = bytesRcvd[beginning_of_payload:]
    data_dict = json.loads(string_received)

    # Update the state in the upcall object
    self.upcall_obj.update_state(data_dict)
      
    return None