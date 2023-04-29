###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.


# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import json # for reading the dht.json file
import random # for choosing a random DHT node to contact

# import serialization logic
from CS6381_MW import discovery_pb2
#from CS6381_MW import topic_pb2  # you will need this eventually

# import any other packages you need.

##################################
#       Broker Middleware class
##################################
class BrokerMW ():

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.logger = logger  # internal logger for print statements
    self.req = None # will be a ZMQ REQ socket to talk to Discovery service
    self.pub = None # will be a ZMQ PUB socket for dissemination
    self.sub = None # will be a ZMQ SUB socket to subscribe to data
    self.poller = None # used to wait on incoming replies
    self.addr = None # our advertised IP address
    self.port = None # port num where we are going to publish our topics
    self.upcall_obj = None # handle to appln obj to handle appln-specific data
    self.handle_events = True # in general we keep going thru the event loop
    self.timeout = None

    # Zookeeper-related fields
    self.disc_sub_socket = None # Socket for subscribing to updates from discovery
    # self.discovery_leader_addr = None 
    # self.discovery_leader_port = None
    # self.discovery_leader_sync_port = None
    self.ipports_connected_to = set()

  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.debug ("BrokerMW::configure")

      # First retrieve our advertised IP addr and the publication port num
      self.port = args.port
      self.addr = args.addr
      self.timeout = args.timeout * 1000 # timeout for receiving data when subscribed in ms

      # path to the DHT.json file
      self.dht_json_path = args.dht_json_path
      
      # Next get the ZMQ context
      self.logger.debug ("BrokerMW::configure - obtain ZMQ context")
      context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("BrokerMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # REQ is needed because we are the client of the Discovery service
      # PUB is needed because we publish topic data
      # SUB is neded for subscribing to all data
      self.logger.debug ("BrokerMW::configure - obtain REQ, PUB, and SUB sockets")
      self.req = context.socket (zmq.REQ)
      self.pub = context.socket (zmq.PUB)
      self.sub = context.socket (zmq.SUB)

      # Register both REQ and SUB sockets with poller
      # Will be handled in different way
      self.logger.debug ("BrokerMW::configure - register the REQ socket for incoming replies")
      self.poller.register (self.req, zmq.POLLIN)
      self.poller.register (self.sub, zmq.POLLIN)
      
      # Now connect ourselves to the discovery service. Recall that the IP/port were
      # supplied in our argument parsing. Best practices of ZQM suggest that the
      # one who maintains the REQ socket should do the "connect"
      self.logger.debug ("BrokerMW::configure - connect to Discovery service")

      # if we are using DHT lookup, we are connecting to a random node in DHT file
      # otherwise, we are connecting to the discovery service specified in the parameters
      if (self.upcall_obj.lookup == "DHT"):
        f = open(self.dht_json_path)
        dht_file = json.load(f) # get dht.hson as a dictionary
        dht_nodes_number = len(dht_file['dht'])
        self.dht_num = dht_nodes_number
        random_index = random.randint(0, dht_nodes_number-1)
        randomly_chosen_dht = dht_file['dht'][random_index]
        self.logger.debug (f"PublisherMW::configure - connect to DHT Discovery service: {randomly_chosen_dht}")
        connect_str = "tcp://" + randomly_chosen_dht['IP'] + ":" + str(randomly_chosen_dht['port'])
        self.req.connect (connect_str)

      elif (self.upcall_obj.lookup == "Centralized"):
        connect_str = "tcp://" + args.discovery
        self.req.connect (connect_str)

      elif (self.upcall_obj.lookup == "ZooKeeper"):
        # Set up a SUB socket to later connect to a discovery
        self.disc_sub_socket = context.socket (zmq.SUB)
        # Add the sub socket to the poller
        self.poller.register (self.disc_sub_socket, zmq.POLLIN)
        
        # Make the subscriber listen for topics sub and unsub
        # sub is for used for notifying subs of new entities they need to subscribe to
        # unsub 
        self.disc_sub_socket.setsockopt (zmq.SUBSCRIBE, bytes('sub', 'utf-8'))
        self.disc_sub_socket.setsockopt (zmq.SUBSCRIBE, bytes('unsub', 'utf-8'))
      
      # Since we are the Broker, we "bind" the PUB socket
      self.logger.debug ("BrokerMW::configure - bind to the pub socket")
      bind_string = "tcp://*:" + str(self.port)
      self.pub.bind (bind_string)

      # We will connect to publishers via SUB socket when the system is ready and when we make the lookup request
      
      self.logger.info ("BrokerMW::configure completed")

    except Exception as e:
      raise e

  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################
  def event_loop (self, timeout=None):

    try:
      self.logger.info ("BrokerMW::event_loop - run the event loop")

      # we are using a class variable called "handle_events" which is set to
      # True but can be set out of band to False in order to exit this forever
      # loop
      while self.handle_events:  # it starts with a True value
        # poll for events. We give it an infinite timeout.
        # The return value is a socket to event mask mapping
        events = dict (self.poller.poll (timeout=timeout))

        # Unlike the previous starter code, here we are never returning from
        # the event loop but handle everything in the same locus of control
        # Notice, also that after handling the event, we retrieve a new value
        # for timeout which is used in the next iteration of the poll
        
        # check if a timeout has occurred. We know this is the case when
        # the event mask is empty
        if not events:
          # timeout has occurred so it is time for us to make appln-level
          # method invocation. Make an upcall to the generic "invoke_operation"
          # which takes action depending on what state the application
          # object is in.
          timeout = self.upcall_obj.invoke_operation ()
          
        elif self.req in events:  # this is the only socket on which we should be receiving replies

          # handle the incoming reply from remote entity and return the result
          timeout = self.handle_bytes_on_req_socket ()


        elif self.disc_sub_socket in events:
          # Received an update from the discovery leader
          timeout = self.handle_sync_update_from_disc_leader()

        # publishers have published the data
        elif self.sub in events:
            timeout = self.handle_bytes_on_sub_socket ()
          
        else:
          raise Exception ("Unknown event after poll")

      self.logger.info ("BrokerMW::event_loop - out of the event loop")
    except Exception as e:
      raise e
            
  #################################################################
  # handle_bytes_on_req_socket
  #################################################################
  def handle_bytes_on_req_socket (self):
    # REQ socket is only used for talking to the discovery service, so we can safely convert the response to Discovery Response
    try:
      self.logger.info ("BrokerMW::handle_bytes_on_req_socket")

      # let us first receive all the bytes
      bytesRcvd = self.req.recv ()

      # now use protobuf to deserialize the bytes
      # The way to do this is to first allocate the space for the
      # message we expect, here DiscoveryResp and then parse
      # the incoming bytes and populate this structure (via protobuf code)
      disc_resp = discovery_pb2.DiscoveryResp ()
      disc_resp.ParseFromString (bytesRcvd)

      # demultiplex the message based on the message type but let the application
      # object handle the contents as it is best positioned to do so. See how we make
      # the upcall on the application object by using the saved handle to the appln object.
      #
      # Note also that we expect the return value to be the desired timeout to use
      # in the next iteration of the poll.
      if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
        # received a response to our register request
        timeout = self.upcall_obj.handle_register_response (disc_resp.register_resp)
      elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
        # received a response to our isready request
        timeout = self.upcall_obj.handle_isready_response (disc_resp.isready_resp)
      elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS or disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # received a response to our lookup request
        timeout = self.upcall_obj.handle_allpub_lookup_response(disc_resp.lookup_resp)

      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise ValueError ("Unrecognized response message")

      return timeout
    
    except Exception as e:
      raise e

  #################################################################
  # handle_bytes_on_sub_socket
  #################################################################
  def handle_bytes_on_sub_socket (self):
    # received a message from one of the publishers, send it to our subscribers
    try:
      self.logger.debug ("BrokerMW::handle_bytes_on_sub_socket")

      # let us first receive all the bytes
      bytesRcvd = self.sub.recv ()
      data_string = bytesRcvd.decode()
      self.logger.info ("BrokerMW::handle_bytes_on_sub_socket – RECEIVED DATA: %s", data_string)

      self.pub.send (bytesRcvd)
    
      return self.timeout
    
    except Exception as e:
      raise e  

  ########################################
  # register with the discovery service
  #
  # this method is invoked by application object passing the necessary
  # details but then as a middleware object it is our job to do the
  # serialization using the protobuf generated code
  #
  # No return value from this as it is handled in the invoke_operation
  # method of the application object.
  ########################################
  def send_register_request (self, name, group):
    ''' register the appln with the discovery service '''

    try:
      self.logger.info ("BrokerMW::send_register_request")

      # as part of registration with the discovery service, we send
      # what role we are playing, the list of topics we are publishing,
      # and our whereabouts, e.g., name, IP and port

      # The following code shows serialization using the protobuf generated code.

      # Build the Registrant Info message first.
      self.logger.debug ("BrokerMW::register - populate the Registrant Info")
      reg_info = discovery_pb2.RegistrantInfo () # allocate
      reg_info.id = name  # our id
      reg_info.addr = self.addr  # our advertised IP addr where we are publishing
      reg_info.port = self.port # port on which we are publishing
      reg_info.group = group # Group we are assigned to
      self.logger.debug ("BrokerMW::register - done populating the Registrant Info")
      
      # Next build a RegisterReq message
      self.logger.debug ("BrokerMW::register - populate the nested register req")
      register_req = discovery_pb2.RegisterReq ()  # allocate 
      register_req.role = discovery_pb2.ROLE_BOTH  # we are a Broker
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      register_req.info.CopyFrom (reg_info)  # copy contents of inner structure
      register_req.topiclist[:] = []   # empty because we are subscribing to all topics that exist
      self.logger.debug ("BrokerMW::register - done populating nested RegisterReq")

      # Finally, build the outer layer DiscoveryReq Message
      self.logger.debug ("BrokerMW::register - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()  # allocate
      disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.register_req.CopyFrom (register_req)
      self.logger.debug ("BrokerMW::register - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("BrokerMW::register - send stringified buffer to Discovery service")
      self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

      # now go to our event loop to receive a response to this request
      self.logger.info ("BrokerMW::register - sent register message and now now wait for reply")
    
    except Exception as e:
      raise e


  ########################################
  # check if the discovery service gives us a green signal to proceed
  #
  # Here we send the isready message and do the serialization
  #
  # No return value from this as it is handled in the invoke_operation
  # method of the application object.
  ########################################
  def send_isready_request (self):
    ''' check if the system is ready '''

    try:
      self.logger.info ("BrokerMW::is_ready")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler as the message format is very simple.
      # Then send the request to the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("BrokerMW::is_ready - populate the nested IsReady msg")
      isready_req = discovery_pb2.IsReadyReq ()  # allocate 
      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("BrokerMW::is_ready - done populating nested IsReady msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("BrokerMW::is_ready - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_ISREADY
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.isready_req.CopyFrom (isready_req)
      self.logger.debug ("BrokerMW::is_ready - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("BrokerMW::is_ready - send stringified buffer to Discovery service")
      self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
      
      # now go to our event loop to receive a response to this request
      self.logger.info ("BrokerMW::is_ready - request sent and now wait for reply")
      
    except Exception as e:
      raise e

  ########################################
  # send_allpub_lookup_request
  ########################################
  def send_allpub_lookup_request (self):
    ''' send a request to retrieve all publishers '''

    try:
      self.logger.info ("BrokerMW::send_allpub_lookup_request")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler as the message format is very simple.
      # Then send the request to the discovery service
    
      # The following code shows serialization using the protobuf generated code.
      
      # first build a IsReady message
      self.logger.debug ("BrokerMW::send_allpub_lookup_request - populate the nested Lookup msg")
      allpub_lookup_req = discovery_pb2.LookupPubByTopicReq ()  # allocate 
      allpub_lookup_req.topiclist[:] = []
      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("BrokerMW::send_allpub_lookup_request - done populating nested Lookup msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("BrokerMW::send_allpub_lookup_request - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.lookup_req.CopyFrom (allpub_lookup_req)
      self.logger.debug ("BrokerMW::send_allpub_lookup_request - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("BrokerMW::send_allpub_lookup_request - send stringified buffer to Discovery service")
      self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
      
      # now go to our event loop to receive a response to this request
      self.logger.info ("BrokerMW::send_allpub_lookup_request - request sent and now wait for reply")
      
    except Exception as e:
      raise e
    


  ########################################
  # send lookup request to get info about 
  # publishers we need to connect to
  ########################################
  def send_lookup_request (self, topiclist):
    ''' send_lookup_request '''

    try:
      self.logger.debug ("BrokerMW::lookup")

      
      # Next build a RegisterReq message
      self.logger.debug ("BrokerMW::lookup - populate the nested register req")
      lookup_req = discovery_pb2.LookupPubByTopicReq ()  # allocate 
      lookup_req.topiclist[:] = topiclist   # this is how repeated entries are added (or use append() or extend ()
      lookup_req.requester = 'Broker'
      self.logger.debug ("BrokerMW::lookup - done populating nested RegisterReq")

      # Finally, build the outer layer DiscoveryReq Message
      self.logger.debug ("BrokerMW::lookup - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()  # allocate
      disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.lookup_req.CopyFrom (lookup_req)
      # disc_req.timestamp_sent = str(time.time())
      self.logger.debug ("BrokerMW::lookup - done building the outer message")
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to our discovery service
      self.logger.debug ("BrokerMW::lookup - send stringified buffer to Discovery service")
      self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes

      # now go to our event loop to receive a response to this request
      self.logger.debug ("BrokerMW::lookup - sent lookup message and now wait for reply")
    
    except Exception as e:
      raise e
  


  ########################################
  # connect_to_publishers
  ########################################
  def connect_to_publishers (self, addressesToConnectTo):
    ''' connect_to_publishers '''

    try:
      self.logger.info ("BrokerMW::connect_to_publishers")

      # connect to every publisher we are interested in
      for ipport in addressesToConnectTo:
        self.sub.connect ("tcp://" + ipport)
        self.ipports_connected_to.add(ipport)

      # Subscribe to all topics
      self.sub.setsockopt (zmq.SUBSCRIBE, bytes("", 'utf-8'))

      self.logger.info ("BrokerMW::connect_to_publishers – Connected to all of them!")
      self.logger.info("BrokerMW::connect_to_publishers – Connected to the following addresses: %s", str(addressesToConnectTo))
    
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
  # connect_to_discovery_leader
  #
  # Connect to discovery leader on REQ and SUB sockets
  ########################################
  def connect_to_discovery_leader(self, disc_addr, disc_port, disc_sync_port):
    # Connect the req socket
    self.req.connect('tcp://' + disc_addr + ':' + str(disc_port))

    # Subscribe for updates
    self.disc_sub_socket.connect('tcp://' + disc_addr + ':' + str(disc_sync_port))
    return


  ########################################
  # disconnect_from_old_discovery_leader
  #
  # Disconnect from discovery leader on REQ and SUB sockets
  ########################################
  def disconnect_from_old_discovery_leader(self, old_addr, old_port, old_sub_port):
    # Disconnect REQ socket
    self.req.disconnect('tcp://' + old_addr + ':' + str(old_port))
    # Disconnect SUB socket
    self.disc_sub_socket.disconnect('tcp://' + old_addr + ':' + str(old_sub_port))
    return
  
  ########################################
  # handle_sync_update_from_disc_leader
  #
  # Handle an update received from the discovery leader
  # Subscribe to new entities or disconnect from the ones that died
  ########################################
  def handle_sync_update_from_disc_leader(self):
    bytesRcvd = self.disc_sub_socket.recv()
    bytesRcvd = bytesRcvd.decode('utf-8')

    # determine type of message (sub or unsub) and get the messsage itself
    beginning_of_payload = (bytesRcvd.find(':') + 1)
    update_type = bytesRcvd[:(beginning_of_payload-1)]
    string_received = bytesRcvd[beginning_of_payload:]
    data_dict = json.loads(string_received)

    ipport = data_dict['addr'] + ':' + str(data_dict['port'])

    # New broker or pub has joined
    if (update_type == 'sub'):
      self.logger.info("Processing a SUB update")
      # A new publisher has joined
      if (data_dict['update_type'] == 'pub'):
        # Subscribe if it is publishing on the topics we are interested in
        if(self.list1_contains_an_element_from_list2(data_dict['topics'], self.upcall_obj.topics_assigned)):
          self.connect_to_publishers([ipport])

    # Broker or pub has died
    elif(update_type == 'unsub'):
      self.logger.info("Processing a UNSUB update")
      # unsubscribe if we were subscribed
      if (ipport in self.ipports_connected_to):
        self.sub.disconnect('tcp://' + ipport)
        self.ipports_connected_to.remove(ipport)
        self.logger.info(f"Disconnected from {ipport}")

    return
  
  ########################################
  # list1_contains_an_element_from_list2
  #
  # Returns true if list1 contains any elements from list 2
  ########################################
  def list1_contains_an_element_from_list2(self, list1, list2):
    for item in list2:
      if item in list1:
        return True
    return False