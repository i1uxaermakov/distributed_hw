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

# Objects to interact with Zookeeper
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
import json



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
    self.name = None # our name (some unique name)
    self.lookup = None # one of the diff ways we do lookup
    self.dissemination = None # direct or via broker
    self.mw_obj = None # handle to the underlying Middleware object
    self.logger = logger  # internal logger for print statements
    self.expected_pub_num = 0    # number of publishers in the system
    self.expected_sub_num = 0    # number of subscribers in the system
    self.timeout = None

    self.registered_publishers = set() # set of strings, where each string is id of a publisher
    self.publisher_id_to_ipport_mapping = {}
    self.topic_to_publishers_id_mapping = {} # a dictionary that maps a string representing a topic to an array of strings (ids of publishers disseminating on that topic)

    self.registered_subscribers = set() # set of strings, where each string is id of a subscriber

    self.registered_brokers = set() # set of strings, where each string is ip:port of a broker
    self.broker_id_to_ipport_mapping = {}

    # Zookeeper-related variables
    self.zk_client = None
    self.zk_am_leader = False
    self.addr = None
    self.sub_port = None
    self.broker_leaders = {
      'group1': None,
      'group2': None,
      'group3': None
    }
    self.port = None


  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):

    try:
      # Here we initialize any internal variables
      self.logger.debug ("DiscoveryAppln::configure")

      # set our current state to CONFIGURE state
      self.state = self.State.CONFIGURE

      # Now, get the configuration object
      self.logger.debug ("PublisherAppln::configure - parsing config.ini")
      config = configparser.ConfigParser ()
      config.read (args.config)
      self.lookup = config["Discovery"]["Strategy"]
      self.dissemination = config["Dissemination"]["Strategy"]
      self.timeout = args.timeout * 1000 # timeout for receiving data when subscribed in ms

      self.group_to_topics_mapping = {
        'group1': config['GroupToTopicMapping']['group1'].split(','),
        'group2': config['GroupToTopicMapping']['group2'].split(','),
        'group3': config['GroupToTopicMapping']['group3'].split(',')
      }
      

      self.name = args.name
      self.expected_pub_num = args.publishers    # number of publishers in the system
      self.expected_sub_num = args.subscribers    # number of subscribers in the system

      # Now setup up our underlying middleware object to which we delegate
      # everything
      self.logger.debug ("PublisherAppln::configure - initialize the middleware object")
      self.mw_obj = DiscoveryMW (self.logger)

      # First ask our middleware to keep a handle to us to make upcalls.
      # This is related to upcalls. By passing a pointer to ourselves, the
      # middleware will keep track of it and any time something must
      # be handled by the application level, invoke an upcall.
      self.logger.debug ("DiscoveryAppln::driver - upcall handle")
      self.mw_obj.set_upcall_handle (self)

      # pass remainder of the args to the m/w object
      self.mw_obj.configure (args) 

      self.addr = args.addr
      self.sub_port = args.sub_port
      self.port = args.port

      # if Zookeeper lookup is used, create /discovery/leader node
      if (self.lookup == 'ZooKeeper'):
        self.zookeeper_addr = args.zookeeper
        self.zk_client = KazooClient(hosts=self.zookeeper_addr)
        self.zk_client.start()

        # Conduct the election of a leader
        self.zk_am_leader = self.register_discovery_with_zookeeper()
        
        # Start a children watch on /discovery
        @self.zk_client.ChildrenWatch('/discovery')
        def watch_discovery_children(children):
          if(len(children) == 0):
            # if we get triggered, this means a discovery node has died, so we need to elect a new discovery leader and subscribe to it if we are not the leader
            self.zk_am_leader = self.register_discovery_with_zookeeper()
          return
        
        # After selecting a leader, subscribe to /pubs, /brokers nodes with child watch
        # Whenever they get triggered, we check whether a new one has joined or a publisher/broker died
        pubs_path = '/pubs'
        self.zk_client.ensure_path(pubs_path)
        @self.zk_client.ChildrenWatch(pubs_path)
        def watch_pubs_children(children):
          self.process_pubs_child_trigger(children)

        # If using Broker dissemination, set up a watch for broker leader too
        if(self.dissemination == 'Broker'):
          brokers_path = '/brokers'
          self.zk_client.ensure_path(brokers_path)
          @self.zk_client.ChildrenWatch(brokers_path)
          def watch_brokers_children(children):
            self.process_brokers_child_trigger(children)


      self.logger.debug ("DiscoveryAppln::configure - configuration complete")
      
    except Exception as e:
      raise e


  ########################################
  # register_discovery_with_zookeeper
  #
  # Return true if we became a leader, false otherwise
  ########################################
  def register_discovery_with_zookeeper(self):
    path = '/discovery/leader'

    # Create an ephemeral node for Discovery leader
    try:
      # Data to store in the node
      data_dict = {
        'addr': self.addr,
        'port': self.port,
        'sub_port': self.sub_port,
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

      # Get data from the node
      data_bytes, _ = self.zk_client.get(path)
      data_dict = json.loads(data_bytes.decode('utf-8'))

      # Subscribe for updates from primary discovery
      self.logger.info (f"Connecting to leader to receive updates: {self.name} at {data_dict['addr']}:{data_dict['sub_port']}")
      self.mw_obj.subscribe_for_updates_from_leader(data_dict['addr'], data_dict['sub_port'])

      return False
      

  ########################################
  # process_pubs_child_trigger
  #
  # Function that is called whenever the 
  # children of /pubs node change
  ########################################
  def process_pubs_child_trigger(self, current_children):
    # Go over current children and check which ones are present in the list of publishers
    self.logger.info('Pubs watch triggered')
    registered_publishers = self.registered_publishers.copy()

    for child in current_children:
      if child not in registered_publishers:
        # a node exists but discovery doesn't know about that publisher
        # we assume this happened because the publisher created a node but hasn't yet registered with the discovery
        # discregard the node since we will notify the brokers/subscribers of the new publisher once they register
        continue
      else:
        # remove child from registered_publishers so that the set only contains the publishers that have registered before but now don't have a node in ZooKeeper
        registered_publishers.remove(child)

    # registered_publishers now contains the publishers that died
    # notify subscribers and brokers of the nodes they need to unsubscribe from
    self.logger.info(f'Died publishers: {registered_publishers}')

    for died_publisher_name in registered_publishers:
      ipport = self.publisher_id_to_ipport_mapping[died_publisher_name]
      beginning_of_port = (ipport.find(':') + 1)
      ip = ipport[:(beginning_of_port-1)]
      port = ipport[beginning_of_port:]

      # Send an update to subscribers and brokers about the publisher that died
      unsub_update = {
        'addr': ip,
        'port': port
      }
      self.mw_obj.publish_unsub_update(unsub_update)

      # Remove the publisher from state
      self.registered_publishers.remove(died_publisher_name)
      del self.publisher_id_to_ipport_mapping[died_publisher_name]
      for topic in self.topic_to_publishers_id_mapping:
        if died_publisher_name in self.topic_to_publishers_id_mapping[topic]:
          self.topic_to_publishers_id_mapping[topic].remove(died_publisher_name)
    
    self.logger.info(f'New State: reg_pubs:{self.registered_publishers}, pub_ipport:{self.publisher_id_to_ipport_mapping}, topic_pubid:{self.topic_to_publishers_id_mapping}')

    # Send an updated state to discovery replicas
    self.mw_obj.publish_discovery_update()

    return
  

  ########################################
  # process_brokers_child_trigger
  #
  # Function that is called whenever the 
  # children of /brokers node change
  ########################################
  def process_brokers_child_trigger(self, current_children):
    new_broker_leaders = {
      'group1': None,
      'group2': None,
      'group3': None,
    }

    # Each child is a leader within a group
    for child in current_children:
      # Get the info of the leader in the group
      # Get data from the node
      data_bytes, _ = self.zk_client.get('/brokers/' + child)
      data_dict = json.loads(data_bytes.decode('utf-8'))
      self.logger.info(f'Current broker leader: {data_dict}')

      brokers_group = child
      new_broker_leaders[brokers_group] = data_dict

    # Check if any of the leaders have changed, send appropriate sub and unsub notifications
    self.check_if_group_leader_changed_and_send_notif('group1', new_broker_leaders)
    self.check_if_group_leader_changed_and_send_notif('group2', new_broker_leaders)
    self.check_if_group_leader_changed_and_send_notif('group3', new_broker_leaders)

    # Update the state of broker leaders
    self.broker_leaders = new_broker_leaders
    

  ########################################
  # check_if_group_leader_changed_and_send_notif
  # Also removes brokers from the local state if necessary
  ########################################
  def check_if_group_leader_changed_and_send_notif(self, group_name, new_broker_leaders):
    # Check if group_name leader changed
    if(self.broker_leaders[group_name]==None):
      if(new_broker_leaders[group_name] != None):
        # There was no broker, now there is one
        # Issue a sub update
        sub_update = {
          'update_type': 'broker',
          'addr': new_broker_leaders[group_name]['addr'],
          'port': new_broker_leaders[group_name]['port'],
          'topics': self.group_to_topics_mapping[group_name]
        }
        self.mw_obj.publish_sub_update(sub_update)

    else:
      # There was a broker
      # It can either be the same (alive), dead, or new

      if(new_broker_leaders[group_name] == None):
        # New broker hasn't been chosen
        # just send an unsub update
        # Send an unsub update for old
        unsub_update = {
          'addr': self.broker_leaders[group_name]['addr'],
          'port': self.broker_leaders[group_name]['port']
        }
        self.mw_obj.publish_unsub_update(unsub_update)

        # Remove from local state
        old_broker_name = self.broker_leaders[group_name]['name']
        self.registered_brokers.remove(old_broker_name)
        del self.broker_id_to_ipport_mapping[old_broker_name]
      
      else:
        # There was a broker and there is one right now
        # Either same or new
        if(self.broker_leaders[group_name]['name'] != new_broker_leaders[group_name]['name']):
          # Broker has changed
          # Send an unsub update for old
          unsub_update = {
            'addr': self.broker_leaders[group_name]['addr'],
            'port': self.broker_leaders[group_name]['port']
          }
          self.mw_obj.publish_unsub_update(unsub_update)

          # Remove from local state
          old_broker_name = self.broker_leaders[group_name]['name']
          self.registered_brokers.remove(old_broker_name)
          del self.broker_id_to_ipport_mapping[old_broker_name]
          
          # Send a sub update for a new broker
          sub_update = {
            'update_type': 'broker',
            'addr': new_broker_leaders[group_name]['addr'],
            'port': new_broker_leaders[group_name]['port'],
            'topics': self.group_to_topics_mapping[group_name]
          }
          self.mw_obj.publish_sub_update(sub_update)

        else:
          # Same broker as before, don't do anything
          pass
    return


  ########################################
  # driver program
  ########################################
  def driver (self):
    ''' Driver program for Discovery Service'''

    try:
      self.logger.debug ("DiscoveryAppln::driver")

      # dump our contents (debugging purposes)
      self.dump ()

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
      
      self.logger.debug ("DiscoveryAppln::driver completed")
      
    except Exception as e:
      raise e

  ########################################
  # handle_register_request
  ########################################
  def handle_register_request (self, register_req, framesRcvd, timestamp_sent):
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
          self.mw_obj.respond_to_register_request(framesRcvd, False, reason.format(pub_id = registrant_id), timestamp_sent)
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
          self.mw_obj.respond_to_register_request(framesRcvd, True, "", timestamp_sent)

          # Notify subscribers and brokers of a new publisher
          if (self.lookup == 'ZooKeeper'):
            # Publish a sub update
            # If you need any of these topics, subscribe to this 
            sub_update = {
              'update_type': 'pub',
              'addr': registrant_ip,
              'port': registrant_port,
              'topics': list(topiclist)
            }
            self.mw_obj.publish_sub_update(sub_update)
      
      elif (register_req.role == discovery_pb2.ROLE_SUBSCRIBER):
        # A SUBSCRIBER sent a request to register
        self.logger.debug ("DiscoveryAppln::handle_register_request - A subscriber sent a request to register")
        
        # check if subscriber with the same ID is already registered
        if(registrant_id in self.registered_subscribers):
          reason = "Subscriber with ID={sub_id:s} already registered"
          self.mw_obj.respond_to_register_request(framesRcvd, False, reason.format(sub_id = registrant_id), timestamp_sent)
        else:
          # Add subscriber to the list of subscribers
          self.registered_subscribers.add(registrant_id)
          self.mw_obj.respond_to_register_request(framesRcvd, True, "", timestamp_sent)

      elif (register_req.role == discovery_pb2.ROLE_BOTH): 
        # BROKER sent a request to register
        self.logger.debug ("DiscoveryAppln::handle_register_request - A broker sent a request to register")

        if(registrant_id in self.registered_brokers):
          reason = "Broker with ID={broker_id:s} already registered"
          self.mw_obj.respond_to_register_request(framesRcvd, False, reason.format(broker_id = registrant_id), timestamp_sent)
        else:
          # Add broker to the list of brokers
          # TODO
          self.registered_brokers.add(registrant_id)
          self.broker_id_to_ipport_mapping[registrant_id] = ip_port_pair
          self.mw_obj.respond_to_register_request(framesRcvd, True, "", timestamp_sent)

      else:
        # Request with unknown role has been received, abort
        self.logger.debug ("DiscoveryAppln::handle_register_request - Register Request with unknown role has been received, abort")
        raise ValueError ("DiscoveryAppln::handle_register_request - Register Request with unknown role has been received, abort")


      if (self.lookup == 'ZooKeeper'):
        # Send an update of state on pub socket
        self.mw_obj.publish_discovery_update()

      return None

    except Exception as e:
      raise e


  ########################################
  # handle_isready_request
  ########################################
  def handle_isready_request(self, isready_request_body, framesRcvd, timestamp_sent):
    if (self.lookup == 'ZooKeeper'):
      # When using Zookeeper, the system is always ready
      # Send the response with True 
      self.mw_obj.respond_to_isready_request(True, framesRcvd, timestamp_sent)
      return None
    
    elif (self.lookup == 'DHT'):
      dht_payload = isready_request_body.dht_payload
      visited_nodes_set = set(dht_payload.visited_nodes)
      registered_subs_set = set(dht_payload.registered_subs)
      registered_pubs_set = set(dht_payload.registered_pubs)
      registered_brokers_set = set(dht_payload.registered_brokers)

      if(self.name in visited_nodes_set):
        # We have done a full circle, now check if the system is ready
        areSubscribersReady = (self.expected_sub_num == len(registered_subs_set))
        arePublishersReady = (self.expected_pub_num == len(registered_pubs_set))

        areBrokersReady = (self.dissemination != 'Broker' or (self.dissemination == 'Broker' and len(registered_brokers_set) != 0))

        self.logger.debug("areBrokersReady = %s", str(areBrokersReady))

        isSystemReady = areSubscribersReady and arePublishersReady and areBrokersReady

        # send the response with the result
        self.mw_obj.respond_to_isready_request(isSystemReady, framesRcvd, timestamp_sent)

        self.logger.debug(f"IS_READY, producers: {str(registered_pubs_set)} ")
        self.logger.debug(f"IS_READY, subscribers: {str(registered_subs_set)} ")
        self.logger.debug(f"IS_READY, brokers: {str(registered_brokers_set)} ")
        self.logger.debug(f"IS_READY, visited nodes: {str(visited_nodes_set)} ")

        return None

      else:
        # Haven't done a full circle yet. Keep going
        # Add yourself to the set
        visited_nodes_set.add(self.name)

        # Add registered publishers
        registered_pubs_set.update(self.registered_publishers)

        # Add registered subscribers
        registered_subs_set.update(self.registered_subscribers)

        # Add registered publishers
        registered_brokers_set.update(self.registered_brokers)

        # Send the is_ready request to the next node
        self.mw_obj.forward_isready_request_further(visited_nodes_set, registered_pubs_set, registered_subs_set, registered_brokers_set, framesRcvd, timestamp_sent)

        # Timeout is None
        return None


    # Not using DHT (Using other method of lookup)
    else:
      # The system is ready when all subscribers, publishers, and brokers (if disseminating through brokers) have registered themselves with discovery service
      areSubscribersReady = self.expected_sub_num == len(self.registered_subscribers)
      arePublishersReady = self.expected_pub_num == len(self.registered_publishers)

      areBrokersReady = (self.dissemination != 'Broker' or (self.dissemination == 'Broker' and len(self.registered_brokers) != 0))

      self.logger.debug("areBrokersReady = %s", str(areBrokersReady))

      isSystemReady = areSubscribersReady and arePublishersReady and areBrokersReady

      # send the response with the result
      self.mw_obj.respond_to_isready_request(isSystemReady, framesRcvd, timestamp_sent)

      return None
  

  ########################################
  # handle_lookup_pub_by_topics
  ########################################
  def handle_lookup_pub_by_topics(self, lookup_req, all, framesRcvd, timestamp_sent):
    ''' handle lookup request '''

    try:
      self.logger.info ("DiscoveryAppln::handle_lookup_pub_by_topics")

      # Set of strings of ip:port to connect to in order to receive data from needed topics
      socketsToConnectTo = set()
      
      if(self.dissemination == 'Broker' and not all):
        if(lookup_req.requester == 'Broker'):
          # Requested by broker, so we send back the publishers
          # based on the group of the broker, we return the publishers that publish on their topics
          self.logger.info ("Handling a lookup request by a broker")
          for topic in lookup_req.topiclist:
            if self.topic_to_publishers_id_mapping.get(topic) is not None:
              for pub_id in self.topic_to_publishers_id_mapping.get(topic):
                socketsToConnectTo.add(self.publisher_id_to_ipport_mapping[pub_id])

        else:
          # Requested by a subscriber, we send back the brokers they need to subscribe to
          # return all brokers since the only registered brokers are going to be the leader ones
          self.logger.info ("Handling a lookup request by a subscriber")
          for broker_name in self.registered_brokers:
            broker_ip_port = self.broker_id_to_ipport_mapping[broker_name]
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
            if self.topic_to_publishers_id_mapping.get(topic) is not None:
              for pub_id in self.topic_to_publishers_id_mapping.get(topic):
                socketsToConnectTo.add(self.publisher_id_to_ipport_mapping[pub_id])

        self.logger.info ("DiscoveryAppln::handle_lookup_pub_by_topics – for topics %s the subscriber will need to connect to the following publishers %s", str(lookup_req.topiclist), str(socketsToConnectTo))

      # socketsToConnectTo set now contains all sockets the subscriber needs to connect to (based on all of the info this discovery node has)

      # Now if we visited all nodes, we just send it back
      # If we have more nodes to visit, we forward the lookup request further

      if(self.lookup == 'Centralized' or self.lookup == 'ZooKeeper'):
        # Send them to the requester
        self.mw_obj.respond_to_lookup_request(socketsToConnectTo, all, framesRcvd, timestamp_sent)
      
      elif (self.lookup == 'DHT'):
        # check if we visited all nodes
        visited_nodes_set = set(lookup_req.visited_nodes)
        already_added_sockets = set(lookup_req.sockets_to_connect_to)
        already_added_sockets.update(socketsToConnectTo)

        if (self.name in visited_nodes_set):
          # Did the full circle, just send everything back
          self.mw_obj.respond_to_lookup_request(already_added_sockets, all, framesRcvd, timestamp_sent)    
        else:
          # Haven't done the full circle, forward the request to the next node
          visited_nodes_set.add(self.name)
          self.mw_obj.forward_lookup_request_further(visited_nodes_set, already_added_sockets, lookup_req.topiclist, all, framesRcvd, timestamp_sent)
      
      return None

    except Exception as e:
      raise e
    
  ########################################
  # update_state
  ########################################
  def update_state(self, new_state):
    self.registered_publishers = set(new_state['registered_publishers'])
    self.publisher_id_to_ipport_mapping = new_state['publisher_id_to_ipport_mapping']
    self.topic_to_publishers_id_mapping = new_state['topic_to_publishers_id_mapping']
    self.registered_subscribers = set(new_state['registered_subscribers'])
    self.registered_brokers = set(new_state['registered_brokers'])
    self.broker_id_to_ipport_mapping = new_state['broker_id_to_ipport_mapping']
    self.logger.info("Updated all of the state")




  def stop_appln(self):
    self.logger.info ("PublisherAppln::stop_appln - Stopping the application completed")
    self.mw_obj.disable_event_loop ()
    return None



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
      
      # print out finger table
      self.logger.info ("     Finger Table:")
      for idx, entry in enumerate(self.mw_obj.finger_table):
        self.logger.info(f"          {idx}: hash {entry.hash}, name {entry.node_info['id']}")

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

  parser.add_argument ("-t", "--timeout", type=int, default=30, help="Timeout for receiving requests. If we do not receive requests for this many seconds, we assume publishers/subscribers no longer need discovery service, so we stop the application")

  #dht_json_path
  parser.add_argument ("-j", "--dht_json_path", type=str, default='dht.json', help="Info about dht nodes in the ring.")

  # name of the node
  parser.add_argument ("-n", "--name", type=str, default='discovery', help="Name of the discovery node")

  # address of discovery node
  parser.add_argument("-a", "--addr", default='localhost', help="Address of the Discovery instance")

  # Main port used by the discovery node
  parser.add_argument("-p", "--port", type=int, default=8888, help="Port used by the discovery node")

  # Subscription port used by the discovery node
  parser.add_argument("-s", "--sub_port", type=int, default=8888, help="Port used by the discovery node for disseminating sync information to other discoveries, brokers, subs")
  
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
    logging.debug ("Main - acquire a child logger and then log messages in the child")
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