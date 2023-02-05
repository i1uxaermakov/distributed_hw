###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.

# Import our topic selector. Feel free to use alternate way to
# get your topics of interest
from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.DiscoveryMW import DiscoveryMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in



##################################
#       PublisherAppln class
##################################
class DiscoveryAppln ():

  # these are the states through which our publisher appln object goes thru.
  # We maintain the state so we know where we are in the lifecycle and then
  # take decisions accordingly
  class State (Enum):
    INITIALIZE = 0,
    CONFIGURE = 1,
    WAITING_FOR_REGISTRATIONS = 2,
    SYSTEM_IS_READY = 3,
    COMPLETED = 4

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.state = self.State.INITIALIZE # state that are we in
    self.name = "Discovery" # our name (some unique name)
    self.lookup = None # one of the diff ways we do lookup
    self.dissemination = None # direct or via broker
    self.mw_obj = None # handle to the underlying Middleware object
    self.logger = logger  # internal logger for print statements
    self.expected_pub_num = 0    # number of publishers in the system
    self.expected_sub_num = 0    # number of subscribers in the system

    self.registered_publishers = set() # set of strings, where each string is id of a publisher
    self.publisher_id_to_ipport_mapping = {}
    self.topic_to_publishers_id_mapping = {} # a dictionary that maps a string representing a topic to an array of strings (ids of publishers disseminating on that topic)

    self.registered_subscribers = set() # set of strings, where each string is id of a subscriber

    self.registered_brokers = set() # set of strings, where each string is ip:port of a broker
    self.broker_id_to_ipport_mapping = {}
    

  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):

    try:
      # Here we initialize any internal variables
      self.logger.info ("DiscoveryAppln::configure")

      # set our current state to CONFIGURE state
      self.state = self.State.CONFIGURE

      # Now, get the configuration object
      self.logger.debug ("PublisherAppln::configure - parsing config.ini")
      config = configparser.ConfigParser ()
      config.read (args.config)
      self.lookup = config["Discovery"]["Strategy"]
      self.dissemination = config["Dissemination"]["Strategy"]

      self.expected_pub_num = args.publishers    # number of publishers in the system
      self.expected_sub_num = args.subscribers    # number of subscribers in the system

      # Now setup up our underlying middleware object to which we delegate
      # everything
      self.logger.debug ("PublisherAppln::configure - initialize the middleware object")
      self.mw_obj = DiscoveryMW (self.logger)
      self.mw_obj.configure (args) # pass remainder of the args to the m/w object
      
      self.logger.info ("DiscoveryAppln::configure - configuration complete")
      
    except Exception as e:
      raise e

  ########################################
  # driver program
  ########################################
  def driver (self):
    ''' Driver program for Discovery Service'''

    try:
      self.logger.info ("DiscoveryAppln::driver")

      # dump our contents (debugging purposes)
      self.dump ()

      # First ask our middleware to keep a handle to us to make upcalls.
      # This is related to upcalls. By passing a pointer to ourselves, the
      # middleware will keep track of it and any time something must
      # be handled by the application level, invoke an upcall.
      self.logger.debug ("DiscoveryAppln::driver - upcall handle")
      self.mw_obj.set_upcall_handle (self)

      # the next thing we should be doing is to register with the discovery
      # service. But because we are simply delegating everything to an event loop
      # that will call us back, we will need to know when we get called back as to
      # what should be our next set of actions.  Hence, as a hint, we set our state
      # accordingly so that when we are out of the event loop, we know what
      # operation is to be performed.  In this case we should be registering with
      # the discovery service. So this is our next state.
      self.state = self.State.WAITING_FOR_REGISTRATIONS

      # Now simply let the underlying middleware object enter the event loop
      # to handle events. However, a trick we play here is that we provide a timeout
      # of zero so that control is immediately sent back to us where we can then
      # register with the discovery service and then pass control back to the event loop
      #
      # As a rule, whenever we expect a reply from remote entity, we set timeout to
      # None or some large value, but if we want to send a request ourselves right away,
      # we set timeout is zero.
      #
      self.mw_obj.event_loop (timeout=None)  # start the event loop
      
      self.logger.info ("DiscoveryAppln::driver completed")
      
    except Exception as e:
      raise e

  ########################################
  # handle_register_request
  ########################################
  def handle_register_request (self, register_req):
    ''' handle register request '''

    try:
      self.logger.info ("DiscoveryAppln::handle_register_request")

      registrant_ip = register_req.info.addr
      registrant_port = register_req.info.port
      registrant_id = register_req.info.id
      ip_port_pair = registrant_ip + ":" + str(registrant_port)
      
      if (register_req.role == discovery_pb2.ROLE_PUBLISHER):        
        # A publisher sent a request to register
        self.logger.debug ("DiscoveryAppln::handle_register_request - A publisher sent a request to register")

        # check if publisher with the same ID is already registered
        if(registrant_id in self.registered_publishers):
          reason = "Publisher with ID={pub_id:s} already registered"
          self.mw_obj.respond_to_register_request(False, reason.format(pub_id = registrant_id))
        else:
          # get topics the publisher is going to publish on
          topiclist = register_req.topiclist

          # add publisher to the list of publishers
          self.registered_publishers.add(registrant_id)
          
          # add publisher's ip and port to publisher_id_to_ipport_mapping
          self.publisher_id_to_ipport_mapping[registrant_id] = ip_port_pair
          
          # for each topic the publisher is publishing on, make a note that there is a new publisher in the topic_to_publishers_id_mapping
          for topic in topiclist:
            self.topic_to_publishers_id_mapping.setdefault(topic, []).append(registrant_id)
          
          # respond to the service that made the request
          self.mw_obj.respond_to_register_request(True)
      
      elif (register_req.role == discovery_pb2.ROLE_SUBSCRIBER):
        # A SUBSCRIBER sent a request to register
        self.logger.debug ("DiscoveryAppln::handle_register_request - A subscriber sent a request to register")
        
        # check if subscriber with the same ID is already registered
        if(registrant_id in self.registered_subscribers):
          reason = "Subscriber with ID={sub_id:s} already registered"
          self.mw_obj.respond_to_register_request(False, reason.format(sub_id = registrant_id))
        else:
          # Add subscriber to the list of subscribers
          self.registered_subscribers.add(registrant_id)
          self.mw_obj.respond_to_register_request(True)

      elif (register_req.role == discovery_pb2.ROLE_BOTH): 
        # BROKER sent a request to register
        self.logger.debug ("DiscoveryAppln::handle_register_request - A broker sent a request to register")

        if(registrant_id in self.registered_brokers):
          reason = "Broker with ID={broker_id:s} already registered"
          self.mw_obj.respond_to_register_request(False, reason.format(broker_id = registrant_id))
        else:
          # Add broker to the list of brokers
          self.registered_brokers.add(registrant_id)
          self.broker_id_to_ipport_mapping[registrant_id] = ip_port_pair
          self.mw_obj.respond_to_register_request(True)

      else:
        # Request with unknown role has been received, abort
        self.logger.debug ("DiscoveryAppln::handle_register_request - Register Request with unknown role has been received, abort")
        raise ValueError ("DiscoveryAppln::handle_register_request - Register Request with unknown role has been received, abort")

      return None

    except Exception as e:
      raise e


  ########################################
  # handle_isready_request
  ########################################
  def handle_isready_request(self, isready_request_body):
    # The system is ready when all subscribers, publishers, and brokers (if disseminating through brokers) have registered themselves with discovery service
    areSubscribersReady = self.expected_sub_num == len(self.registered_subscribers)
    arePublishersReady = self.expected_pub_num == len(self.registered_publishers)

    areBrokersReady = (self.dissemination != 'Broker' or (self.dissemination == 'Broker' and len(self.registered_brokers) != 0))

    isSystemReady = areSubscribersReady and arePublishersReady and areBrokersReady

    # send the response with the result
    self.mw_obj.respond_to_isready_request(isSystemReady)

    return None
  

  ########################################
  # handle_lookup_pub_by_topics
  ########################################
  def handle_lookup_pub_by_topics(self, lookup_req, all=False):
    ''' handle lookup request '''

    try:
      self.logger.info ("DiscoveryAppln::handle_lookup_pub_by_topics")

      # Set of strings of ip:port to connect to in order to receive data from needed topics
      socketsToConnectTo = set()
      
      # if disseminating through broker, we are returning the ip:port of a broker, no matter what the subscriber is interested in
      if(self.dissemination == 'Broker' and not all):
        self.logger.info ("DiscoveryAppln::handle_lookup_pub_by_topics – disseminating through broker, returning broker's address")
        brokerId = next(iter(self.registered_brokers))
        broker_ip_port = self.broker_id_to_ipport_mapping[brokerId]
        socketsToConnectTo.add(broker_ip_port)
      
      # if disseminating directly, return ip:port of publishers that publish on those topics
      else:
        self.logger.info ("DiscoveryAppln::handle_lookup_pub_by_topics – disseminating through direct approach, finding subscribers to talk to")
        if(all):
          # if interested in all topics (in case of broker's lookup request), then go over all publishers and add their ip:port
          for publisher_ip_port in self.publisher_id_to_ipport_mapping.values():
            socketsToConnectTo.add(publisher_ip_port)
        else:
          # If interested in select number of topics, go over topics and add corresponding publishers to the set of publishers to connect to
          for topic in lookup_req.topiclist:
            for pub_id in self.topic_to_publishers_id_mapping[topic]:
              socketsToConnectTo.add(self.publisher_id_to_ipport_mapping[pub_id])

        self.logger.info ("DiscoveryAppln::handle_lookup_pub_by_topics – for topics %s the subscriber will need to connect to the following publishers %s", str(lookup_req.topiclist), str(socketsToConnectTo))

      # socketsToConnectTo set now contains all sockets the subscriber needs to connect to
      # Send them to the requester
      self.mw_obj.respond_to_lookup_request(socketsToConnectTo)
      
      return None

    except Exception as e:
      raise e


  ########################################
  # dump the contents of the object 
  ########################################
  def dump (self):
    ''' Pretty print '''

    try:
      self.logger.info ("**********************************")
      self.logger.info ("DiscoveryAppln::dump")
      self.logger.info ("------------------------------")
      self.logger.info ("     Name: {}".format (self.name))
      self.logger.info ("     Lookup: {}".format (self.lookup))
      self.logger.info ("     Dissemination: {}".format (self.dissemination))
      self.logger.info ("     Number of publishers in the system: {}".format (self.expected_pub_num))
      self.logger.info ("     Number of subscribers in the system: {}".format (self.expected_sub_num))
      self.logger.info ("**********************************")
      
    except Exception as e:
      raise e



###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs ():
  # instantiate a ArgumentParser object
  parser = argparse.ArgumentParser (description="Publisher Application")

  parser.add_argument("-P", "--publishers", type=int, default=2, help="Number of publishers")

  parser.add_argument("-S", "--subscribers", type=int, default=2, help="Number of subscribers")

  parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

  parser.add_argument ("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
  
  return parser.parse_args()



###################################
#
# Main program
#
###################################
def main ():
  try:
    # obtain a system wide logger and initialize it to debug level to begin with
    logging.info ("Main - acquire a child logger and then log messages in the child")
    logger = logging.getLogger ("DiscoveryAppln")
    
    # first parse the arguments
    logger.debug ("Main: parse command line arguments")
    args = parseCmdLineArgs ()

    # reset the log level to as specified
    logger.debug ("Main: resetting log level to {}".format (args.loglevel))
    logger.setLevel (args.loglevel)
    logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

    # Obtain a Discover application
    logger.debug ("Main: obtain the publisher appln object")
    discovery_app = DiscoveryAppln (logger)

    # configure the object
    logger.debug ("Main: configure the publisher appln object")
    discovery_app.configure (args)

    # now invoke the driver program
    logger.debug ("Main: invoke the publisher appln driver")
    discovery_app.driver ()

  except Exception as e:
    logger.error ("Exception caught in main - {}".format (e))
    type, value, traceback = sys.exc_info()
    logger.error ("Type: %s", type)
    logger.error ("Value: %s", value)
    logger.error ("Traceback: %s", traceback.format_exc())
    return


###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":

  # set underlying default logging capabilities
  logging.basicConfig (level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


  main ()