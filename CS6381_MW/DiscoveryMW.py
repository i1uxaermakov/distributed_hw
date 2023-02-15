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

from CS6381_MW import discovery_pb2

class DiscoveryMW ():

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.logger = logger  # internal logger for print statements
    self.rep = None # will be a ZMQ REP socket to receive requests and respond to them
    self.poller = None # used to wait on incoming replies
    self.upcall_obj = None # handle to appln obj to handle appln-specific data
    self.handle_events = True # in general we keep going thru the event loop


  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.info ("DiscoveryMW::configure")
      
      # Next get the ZMQ context
      self.logger.debug ("DiscoveryMW::configure - obtain ZMQ context")
      context = zmq.Context ()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug ("DiscoveryMW::configure - obtain the poller")
      self.poller = zmq.Poller ()
      
      # Now acquire the RES
      # RES is needed because everyone is just sending requests to us and we need to respond
      self.logger.debug ("DiscoveryMW::configure - obtain RES socket")
      self.rep = context.socket (zmq.REP)

      # Since are using the event loop approach, register the REQ socket for incoming events
      # Note that nothing ever will be received on the PUB socket and so it does not make
      # any sense to register it with the poller for an incoming message.
      self.logger.debug ("DiscoveryMW::configure - register the REQ socket for incoming replies")
      self.poller.register (self.rep, zmq.POLLIN)
      
      # Now bind to the socket for incoming requests. We are ready to accept requests from anyone, so the string is tcp://*:*
      self.logger.debug ("DiscoveryMW::configure - bind to the socket and port")
      bind_str = "tcp://*:5555"
      self.rep.bind (bind_str)
      
      self.logger.info ("DiscoveryMW::configure completed")

    except Exception as e:
      raise e


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
        
        # check if a timeout has occurred. We know this is the case when
        # the event mask is empty
        if not events:
          # we are ready to shut down because everybody has already registered
          timeout = self.upcall_obj.stop_appln()
          
        elif self.rep in events:  # this is the only socket on which we should be receiving replies

          # handle the incoming request and respond to it
          timeout = self.handle_request ()
          
        else:
          raise Exception ("Unknown event after poll")

      self.logger.info ("DiscoveryMW::event_loop - out of the event loop")
    except Exception as e:
      raise e


  #################################################################
  # handle an incoming request
  #################################################################
  def handle_request (self):

    try:
      self.logger.info ("DiscoveryMW::handle_request")

      # let us first receive all the bytes
      bytesRcvd = self.rep.recv ()
      self.logger.info ("DiscoveryMW::handle_request – received bytes")

      # now use protobuf to deserialize the bytes
      # The way to do this is to first allocate the space for the
      # message we expect, here DiscoveryResp and then parse
      # the incoming bytes and populate this structure (via protobuf code)
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.ParseFromString (bytesRcvd)
      self.logger.info ("DiscoveryMW::handle_request – converted bytes into DiscoveryReq object")

      # demultiplex the message based on the message type but let the application
      # object handle the contents as it is best positioned to do so.
      # Note also that we expect the return value to be the desired timeout to use
      # in the next iteration of the poll.
      if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
        # let the appln level object decide what to do
        self.logger.info ("DiscoveryMW::handle_request – sending the REGISTER request to be handled in the upcall object")
        timeout = self.upcall_obj.handle_register_request (disc_req.register_req)
      
      elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
        # this is a response to is ready request
        self.logger.info ("DiscoveryMW::handle_request – sending the ISREADY request to be handled in the upcall object")
        timeout = self.upcall_obj.handle_isready_request (disc_req.isready_req)
      
      elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        # received a lookup request
        self.logger.info ("DiscoveryMW::handle_request – sending the LOOKUP PUBLISHERS BY TOPICS request to be handled in the upcall object")
        timeout = self.upcall_obj.handle_lookup_pub_by_topics(disc_req.lookup_req)

      elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
        # received a lookup request
        self.logger.info ("DiscoveryMW::handle_request – sending the LOOKUP ALL PUBLISHERS request to be handled in the upcall object")
        timeout = self.upcall_obj.handle_lookup_pub_by_topics(disc_req.lookup_req, True)

      else: # anything else is unrecognizable by this object
        # raise an exception here
        raise ValueError ("Unrecognized response message")

      return timeout
    
    except Exception as e:
      raise e


  
  ########################################
  # respond_to_register_request
  #
  # Respond to register request. was_successful indicates whether the 
  # request for registration was successful. If it wasn't, reason variable 
  # is going to have the reason for failure
  ########################################
  def respond_to_register_request(self, was_successful, reason=""):
    ''' respond_to_register_request '''

    try:
      self.logger.info ("DiscoveryMW::respond_to_register_request")
    
      # Create the payload for register response
      register_response = discovery_pb2.RegisterResp()
      if(was_successful):
        register_response.status = discovery_pb2.STATUS_SUCCESS
      else:
        register_response.status = discovery_pb2.FAILURE
        register_response.reason = reason

      # Finally, build the outer layer DiscoveryResp Message
      disc_resp = discovery_pb2.DiscoveryResp ()  # allocate
      disc_resp.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.register_resp.CopyFrom (register_response)
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to the service that sent the request
      self.logger.debug ("DiscoveryMW::respond_to_register_request - send isready stringified buffer to service that sent the request")
      self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
    
    except Exception as e:
      raise e
    

  ########################################
  # respond_to_isready_request
  #
  ########################################
  def respond_to_isready_request(self, is_system_ready):
    ''' respond_to_isready_request '''

    try:
      self.logger.info ("DiscoveryMW::respond_to_isready_request")
    
      # Create the payload for register response
      isready_response = discovery_pb2.IsReadyResp()
      isready_response.status = is_system_ready

      # Finally, build the outer layer DiscoveryResp Message
      disc_resp = discovery_pb2.DiscoveryResp ()  # allocate
      disc_resp.msg_type = discovery_pb2.TYPE_ISREADY  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_resp.isready_resp.CopyFrom (isready_response)
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to the service that sent the request
      self.logger.debug ("DiscoveryMW::respond_to_isready_request - send isready stringified buffer to the service that requested")
      self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
    
    except Exception as e:
      raise e




  ########################################
  # respond_to_lookup_request
  ########################################
  def respond_to_lookup_request(self, publisher_ipports, all=False):
    ''' respond_to_lookup_request '''

    try:
      self.logger.info ("DiscoveryMW::respond_to_lookup_request")
    
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
      
      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_resp.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      # now send this to the service that sent the request
      self.logger.debug ("DiscoveryMW::respond_to_lookup_request - send lookup_response stringified buffer to the service that requested")
      self.rep.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
    
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