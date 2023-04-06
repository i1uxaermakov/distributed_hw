###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 

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
from CS6381_MW.BrokerMW import BrokerMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in

# Objects to interact with Zookeeper
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
import json


##################################
#       BrokerAppln class
##################################
class BrokerAppln ():

  # these are the states through which our publisher appln object goes thru.
  # We maintain the state so we know where we are in the lifecycle and then
  # take decisions accordingly
  class State (Enum):
    INITIALIZE = 0,
    CONFIGURE = 1,
    REGISTER = 2,
    ISREADY = 3,
    LOOKUP_ALL_PUBLISHERS = 4,
    RECEIVE_AND_DISSEMINATE = 5,
    COMPLETED = 6

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.state = self.State.INITIALIZE # state that are we in
    self.name = None # our name (some unique name)
    self.lookup = None # one of the diff ways we do lookup
    self.dissemination = None # direct or via broker
    self.mw_obj = None # handle to the underlying Middleware object
    self.logger = logger  # internal logger for print statements
    self.timeout = None 

    # Zookeeper-related variables
    self.zk_client = None
    self.discovery = None
    self.discovery_leader_addr = None 
    self.discovery_leader_port = None
    self.discovery_leader_sync_port = None
    self.addr = None
    self.port = None
    self.zk_am_broker_leader = False

  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.info ("BrokerAppln::configure")

      # set our current state to CONFIGURE state
      self.state = self.State.CONFIGURE
      
      # initialize our variables
      self.name = args.name # our name
      self.timeout = args.timeout * 1000 # timeout for receiving data when subscribed in ms
      self.addr = args.addr
      self.port = args.port

      # Now, get the configuration object
      self.logger.debug ("BrokerAppln::configure - parsing config.ini")
      config = configparser.ConfigParser ()
      config.read (args.config)
      self.lookup = config["Discovery"]["Strategy"]
      self.dissemination = config["Dissemination"]["Strategy"]

      # Now setup up our underlying middleware object to which we delegate
      # everything
      self.logger.debug ("BrokerAppln::configure - initialize the middleware object")
      self.mw_obj = BrokerMW (self.logger)
      self.mw_obj.set_upcall_handle (self)
      self.mw_obj.configure (args) # pass remainder of the args to the m/w object

      # Connect to Zookeeper to establish connection with Primary Discovery
      if(self.lookup == 'ZooKeeper'):
        self.zookeeper_addr = args.zookeeper
        self.zk_client = KazooClient(hosts=self.zookeeper_addr)
        self.zk_client.start()

        # Do an election for primary broker
        # If not leader, keep spinning here until you are a leader
        # Then proceed to getting discovery, registering, lookup, etc.
        self.conduct_broker_leader_election()

        while not self.zk_am_broker_leader:
          time.sleep(0.5)

        # Get discovery Service
        self.set_up_connection_to_discovery_leader()

        # Set up a watch for the primary discovery
        self.set_up_watch_for_primary_discovery()
      
      self.logger.info ("BrokerAppln::configure - configuration complete")
      
    except Exception as e:
      raise e

  ########################################
  # conduct_broker_leader_election
  ########################################
  def conduct_broker_leader_election(self):
    self.zk_am_broker_leader = self.register_broker_with_zookeeper()

    # Start a children watch on /discovery
    @self.zk_client.ChildrenWatch('/brokers')
    def watch_discovery_children(children):
      if(len(children) == 0):
        # if we get triggered, this means a discovery node has died, so we need to elect a new discovery leader and subscribe to it if we are not the leader
        self.zk_am_broker_leader = self.register_broker_with_zookeeper()

    return
  

  ########################################
  # register_broker_with_zookeeper
  ########################################
  def register_broker_with_zookeeper(self):
    path = '/brokers/leader'

    # Create an ephemeral node for Discovery leader
    try:
      # Data to store in the node
      data_dict = {
        'addr': self.addr,
        'port': self.port,
        'name': self.name
      }
      data_bytes = json.dumps(data_dict).encode('utf-8')

      # Try to create the ephemeral node
      self.zk_client.create(path, ephemeral=True, makepath=True, value=data_bytes)
      self.logger.info ("Ephemeral /discovery/leader successfully created, we are a leader")
      return True
    
    except NodeExistsError:
      # Handle the case where the node already exists
      self.logger.info ("Ephemeral /discovery/leader node already exists, we are NOT a leader")

      return False


  ########################################
  # set_up_connection_to_discovery_leader
  ########################################
  def set_up_connection_to_discovery_leader(self):
    # If exists, get value, set up watch
    # If doesn't exist, spin until it is created
    discovery_leader_path = '/discovery/leader'

    # spin while we are waiting for primary discovery to appear
    while True:
      if(self.zk_client.exists(discovery_leader_path)):
        # Retrieve info about the leader
        leader_info = self.get_data_about_discovery_leader()

        # update discovery leader info
        self.discovery_leader_addr = leader_info['addr']
        self.discovery_leader_port = leader_info['port']
        self.discovery_leader_sync_port = leader_info['sub_port']
        
        # Connect to the new discovery and subscribe for updates from it
        self.mw_obj.connect_to_discovery_leader(leader_info['addr'], leader_info['port'], leader_info['sub_port'])
        break

      else:
        # Node does not exist yet, so we wait
        time.sleep(1)


  ########################################
  # set_up_watch_for_primary_discovery
  ########################################
  def set_up_watch_for_primary_discovery(self):
    # Set up a watch for discovery leader to get notified when it changes
    @self.zk_client.ChildrenWatch('/discovery')
    def watch_discovery_children(children):
      # No children means the discovery died, so we can disconnect from the old one
      if (len(children) == 0):
        # Disconnect from the old one if there is an old one
        if(self.discovery_leader_addr != None):
          self.mw_obj.disconnect_from_old_discovery_leader(self.discovery_leader_addr, self.discovery_leader_port, self.discovery_leader_sync_port)

        # Update the info about the discovery leader
        self.discovery_leader_addr = None
        self.discovery_leader_port = None
        self.discovery_leader_sync_port = None

        return

      # There is a child, so there is a primary discovery
      else:
        # Retrieve info about the leader
        leader_info = self.get_data_about_discovery_leader()

        # If the leader has changed, connect to the new leader
        if(leader_info['addr'] != self.discovery_leader_addr):
        
          # Disconnect from old one if we are still connected (i.e. )
          if(self.discovery_leader_addr != None):
            self.mw_obj.disconnect_from_old_discovery_leader(self.discovery_leader_addr, self.discovery_leader_port, self.discovery_leader_sync_port)
          
          # Connect to the new discovery and subscribe for updates from it
          self.mw_obj.connect_to_discovery_leader(leader_info['addr'], leader_info['port'], leader_info['sub_port'])

          # Update info about discovery leader
          self.discovery_leader_addr = leader_info['addr'] 
          self.discovery_leader_port = leader_info['port']
          self.discovery_leader_sync_port = leader_info['sub_port']
      return


  ########################################
  # get_data_about_discovery_leader
  ########################################
  def get_data_about_discovery_leader(self):
    disc_leader_data, _ = self.zk_client.get('/discovery/leader')
    data_dict = json.loads(disc_leader_data.decode('utf-8'))
    return data_dict



  ########################################
  # driver program
  ########################################
  def driver (self):
    ''' Driver program '''

    try:
      self.logger.info ("BrokerAppln::driver")

      # dump our contents (debugging purposes)
      self.dump ()

      # the next thing we should be doing is to register with the discovery
      # service. But because we are simply delegating everything to an event loop
      # that will call us back, we will need to know when we get called back as to
      # what should be our next set of actions.  Hence, as a hint, we set our state
      # accordingly so that when we are out of the event loop, we know what
      # operation is to be performed.  In this case we should be registering with
      # the discovery service. So this is our next state.
      self.state = self.State.REGISTER

      # Now simply let the underlying middleware object enter the event loop
      # to handle events. However, a trick we play here is that we provide a timeout
      # of zero so that control is immediately sent back to us where we can then
      # register with the discovery service and then pass control back to the event loop
      #
      # As a rule, whenever we expect a reply from remote entity, we set timeout to
      # None or some large value, but if we want to send a request ourselves right away,
      # we set timeout is zero.
      #
      self.mw_obj.event_loop (timeout=0)  # start the event loop
      
      self.logger.info ("BrokerAppln::driver completed")
      
    except Exception as e:
      raise e

  ########################################
  # generic invoke method called as part of upcall
  #
  # This method will get invoked as part of the upcall made
  # by the middleware's event loop after it sees a timeout has
  # occurred.
  ########################################
  def invoke_operation (self):
    ''' Invoke operating depending on state  '''

    try:
      self.logger.info ("BrokerAppln::invoke_operation")

      # check what state are we in. If we are in REGISTER state,
      # we send register request to discovery service. If we are in
      # ISREADY state, then we keep checking with the discovery
      # service.
      if (self.state == self.State.REGISTER):
        # send a register msg to discovery service
        self.logger.debug ("BrokerAppln::invoke_operation - register with the discovery service")
        self.mw_obj.send_register_request (self.name)

        # Remember that we were invoked by the event loop as part of the upcall.
        # So we are going to return back to it for its next iteration. Because
        # we have just now sent a register request, the very next thing we expect is
        # to receive a response from remote entity. So we need to set the timeout
        # for the next iteration of the event loop to a large num and so return a None.
        return None
      
      elif (self.state == self.State.ISREADY):
        # Now keep checking with the discovery service if we are ready to go
        #
        # Note that in the previous version of the code, we had a loop. But now instead
        # of an explicit loop we are going to go back and forth between the event loop
        # and the upcall until we receive the go ahead from the discovery service.
        
        self.logger.debug ("BrokerAppln::invoke_operation - check if are ready to go")
        self.mw_obj.send_isready_request ()  # send the is_ready? request

        # Remember that we were invoked by the event loop as part of the upcall.
        # So we are going to return back to it for its next iteration. Because
        # we have just now sent a isready request, the very next thing we expect is
        # to receive a response from remote entity. So we need to set the timeout
        # for the next iteration of the event loop to a large num and so return a None.
        return None

      elif (self.state == self.State.LOOKUP_ALL_PUBLISHERS):
        self.logger.debug ("BrokerAppln::invoke_operation - start Disseminating")

        # Send the lookup request to receive info about all publishers
        # we will need to subscribe to all of them
        self.mw_obj.send_allpub_lookup_request()

        # Block until we receive the lookup response
        return None
      
      elif (self.state == self.State.RECEIVE_AND_DISSEMINATE):
        # if we are here, we timed out on our subscriptions
        # did not receive data for the duration of timeout, which means publishers are done sending data
        # we can now transition to shutting the broker down

        self.logger.debug ("BrokerAppln::invoke_operation - Work completed")

        # we are done. So we move to the completed state
        self.state = self.State.COMPLETED

        # return a timeout of zero so that the event loop sends control back to us right away.
        return 0
        
      elif (self.state == self.State.COMPLETED):

        # we are done. Time to break the event loop. So we created this special method on the
        # middleware object to kill its event loop
        self.mw_obj.disable_event_loop ()
        return None

      else:
        raise ValueError ("Undefined state of the appln object")
      
      self.logger.info ("BrokerAppln::invoke_operation completed")
    except Exception as e:
      raise e

  ########################################
  # handle register response method called as part of upcall
  #
  # As mentioned in class, the middleware object can do the reading
  # from socket and deserialization. But it does not know the semantics
  # of the message and what should be done. So it becomes the job
  # of the application. Hence this upcall is made to us.
  ########################################
  def handle_register_response (self, reg_resp):
    ''' handle register response '''

    try:
      self.logger.info ("BrokerAppln::register_response")
      if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
        self.logger.debug ("BrokerAppln::register_response - registration is a success")

        # set our next state to isready so that we can then send the isready message right away
        self.state = self.State.ISREADY
        
        # return a timeout of zero so that the event loop in its next iteration will immediately make
        # an upcall to us
        return 0
      
      else:
        self.logger.debug ("BrokerAppln::register_response - registration is a failure with reason {}".format (reg_resp.reason))
        raise ValueError ("Publisher needs to have unique id")

    except Exception as e:
      raise e

  ########################################
  # handle isready response method called as part of upcall
  #
  # Also a part of upcall handled by application logic
  ########################################
  def handle_isready_response (self, isready_resp):
    ''' handle isready response '''

    try:
      self.logger.info ("BrokerAppln::isready_response")

      # Notice how we get that loop effect with the sleep (10)
      # by an interaction between the event loop and these
      # upcall methods.
      if not isready_resp.status:
        # discovery service is not ready yet
        self.logger.debug ("BrokerAppln::driver - Not ready yet; check again")
        time.sleep (10)  # sleep between calls so that we don't make excessive calls

      else:
        # we got the go ahead
        # set the state to disseminate
        self.state = self.State.LOOKUP_ALL_PUBLISHERS
        
      # return timeout of 0 so event loop calls us back in the invoke_operation
      # method, where we take action based on what state we are in.
      return 0
    
    except Exception as e:
      raise e


  ########################################
  # handle allpub lookup response
  ########################################
  def handle_allpub_lookup_response (self, lookup_resp):
    ''' handle_allpub_lookup_response '''

    try:
      self.logger.info ("BrokerAppln::handle_allpub_lookup_response")

      # Now we subscribe/connect to all publishers
      self.mw_obj.connect_to_publishers(lookup_resp.addressesToConnectTo)

      # Once we are subscribed, we transition to state RECEIVE_AND_DISSEMINATE
      self.state = self.State.RECEIVE_AND_DISSEMINATE
      
      # Set the timeout to standard timeout for receiving data
      # If we don't receive data for that many seconds, we bring the broker down
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
      self.logger.info ("BrokerAppln::dump")
      self.logger.info ("------------------------------")
      self.logger.info ("     Name: {}".format (self.name))
      self.logger.info ("     Lookup: {}".format (self.lookup))
      self.logger.info ("     Dissemination: {}".format (self.dissemination))
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

  parser.add_argument ("-n", "--name", default="pub", help="Some name assigned to us. Keep it unique per publisher")

  parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")

  parser.add_argument ("-p", "--port", type=int, default=5577, help="Port number on which our underlying publisher ZMQ service runs, default=5577")
    
  parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")

  parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

  parser.add_argument ("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

  parser.add_argument ("-t", "--timeout", type=int, default=20, help="Timeout for receiving subscription data. If we do not receive data for this many seconds, we assume publishers are done publishing data and we stop the application")

  #dht_json_path
  parser.add_argument ("-j", "--dht_json_path", type=str, default='dht.json', help="Info about dht nodes in the ring.")

  # address of Zookeeper
  parser.add_argument("-z", "--zookeeper", default='localhost:2181', help="Address of the Zookeeper instance")
  
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
    logger = logging.getLogger ("BrokerAppln")
    
    # first parse the arguments
    logger.debug ("Main: parse command line arguments")
    args = parseCmdLineArgs ()

    # reset the log level to as specified
    logger.debug ("Main: resetting log level to {}".format (args.loglevel))
    logger.setLevel (args.loglevel)
    logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

    # Obtain a Broker application
    logger.debug ("Main: obtain the publisher appln object")
    broker_app = BrokerAppln (logger)

    # configure the object
    logger.debug ("Main: configure the publisher appln object")
    broker_app.configure (args)

    # now invoke the driver program
    logger.debug ("Main: invoke the publisher appln driver")
    broker_app.driver ()

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