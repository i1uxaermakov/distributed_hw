###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the SubscriberAppln. As in the publisher application, the subscriber application
# will maintain a handle to the underlying subscriber middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each publisher that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these publishers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.


# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
import ast # for working with subsrption data (converting it back to dictionary)
import mysql.connector # for working with mysql for analytics

# Import our topic selector. Feel free to use alternate way to
# get your topics of interest
from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.SubscriberMW import SubscriberMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in


##################################
#       SubscriberAppln class
##################################
class SubscriberAppln ():

  # States of subscriber
  class State (Enum):
    INITIALIZE = 0,
    CONFIGURE = 1,
    REGISTER = 2,
    ISREADY = 3,
    LOOKUP_PUBLISHERS = 4,
    RECEIVE_DATA = 5,
    COMPLETED = 6

  ########################################
  # constructor
  ########################################
  def __init__ (self, logger):
    self.state = self.State.INITIALIZE # state that are we in
    self.name = None # our name (some unique name)
    self.topiclist = None # the different topics that we publish on
    self.num_topics = None # total num of topics we publish
    self.lookup = None # one of the diff ways we do lookup
    self.dissemination = None # direct or via broker
    self.mw_obj = None # handle to the underlying Middleware object
    self.logger = logger  # internal logger for print statements
    self.timeout = None
    self.pub_num = None
    self.sub_num = None
    self.freq = None
    self.latency_data = []

  ########################################
  # configure/initialize
  ########################################
  def configure (self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.info ("SubscriberAppln::configure")

      # set our current state to CONFIGURE state
      self.state = self.State.CONFIGURE
      
      # initialize our variables
      self.name = args.name # our name
      self.num_topics = args.num_topics  # total num of topics we publish
      self.timeout = args.timeout * 1000 # timeout for receiving data when subscribed in ms
      self.pub_num = args.publishers
      self.sub_num = args.subscribers
      self.freq = args.frequency

      # Now, get the configuration object
      self.logger.debug ("SubscriberAppln::configure - parsing config.ini")
      config = configparser.ConfigParser ()
      config.read (args.config)
      self.lookup = config["Discovery"]["Strategy"]
      self.dissemination = config["Dissemination"]["Strategy"]
    
      # Now get our topic list of interest
      self.logger.debug ("SubscriberAppln::configure - selecting our topic list")
      ts = TopicSelector ()
      self.topiclist = ts.interest (self.num_topics)  # let topic selector give us the desired num of topics

      # Now setup up our underlying middleware object to which we delegate
      # everything
      self.logger.debug ("SubscriberAppln::configure - initialize the middleware object")
      self.mw_obj = SubscriberMW (self.logger)
      self.mw_obj.configure (args) # pass remainder of the args to the m/w object
      
      self.logger.info ("SubscriberAppln::configure - configuration complete")
      
    except Exception as e:
      raise e
  
  
  ########################################
  # driver program
  ########################################
  def driver (self):
    ''' Driver program '''

    try:
      self.logger.info ("SubscriberAppln::driver")

      # dump our contents (debugging purposes)
      self.dump ()

      # First ask our middleware to keep a handle to us to make upcalls.
      # This is related to upcalls. By passing a pointer to ourselves, the
      # middleware will keep track of it and any time something must
      # be handled by the application level, invoke an upcall.
      self.logger.debug ("SubscriberAppln::driver - upcall handle")
      self.mw_obj.set_upcall_handle (self)

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
      
      self.logger.info ("SubscriberAppln::driver completed")
      
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
      self.logger.info ("SubscriberAppln::invoke_operation")

      # check what state are we in. If we are in REGISTER state,
      # we send register request to discovery service. If we are in
      # ISREADY state, then we keep checking with the discovery
      # service.
      if (self.state == self.State.REGISTER):
        # send a register msg to discovery service
        self.logger.debug ("SubscriberAppln::invoke_operation - register with the discovery service")
        self.mw_obj.send_register_request (self.name, self.topiclist)

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
        
        self.logger.debug ("SubscriberAppln::invoke_operation - check if are ready to go")
        self.mw_obj.send_isready_request ()  # send the is_ready? request

        # Remember that we were invoked by the event loop as part of the upcall.
        # So we are going to return back to it for its next iteration. Because
        # we have just now sent a isready request, the very next thing we expect is
        # to receive a response from remote entity. So we need to set the timeout
        # for the next iteration of the event loop to a large num and so return a None.
        return None
      
      elif (self.state == self.State.LOOKUP_PUBLISHERS):

        # We are here because both registration and is ready is done. So the only thing
        # left for us as a subscriber is to look up publishers we need to connect to
        self.logger.debug ("SubscriberAppln::invoke_operation - look up publishers")

        # delegate the middleware to send a lookup request
        self.mw_obj.send_lookup_request(self.topiclist)

        # return none to wait for the lookup response from discovery
        return None

      elif (self.state == self.State.RECEIVE_DATA):
        # timeout for receiving subscription data has been triggered
        # we can assume that publishers are done publishing data
        # go into completed state
        self.state = self.State.COMPLETED
        return 0
        
      elif (self.state == self.State.COMPLETED):

        # we are done. Time to break the event loop. So we created this special method on the
        # middleware object to kill its event loop
        self.mw_obj.disable_event_loop ()

        # Send data to database
        self.send_latency_data_to_db()

        return None

      else:
        raise ValueError ("Undefined state of the appln object")
      
      self.logger.info ("Subscriber::invoke_operation completed")
    except Exception as e:
      raise e


  ########################################
  # Sending collected data about 
  # latencies for further analysis 
  # into mysql database on aws
  ########################################
  def send_latency_data_to_db(self):
    try:
      connection = mysql.connector.connect(host='34.227.58.170',
                                          database='distributed_hw1',
                                          user='root',
                                          password='Password2023!')

      mySql_insert_query = """INSERT INTO latencies(latency_sec, frequency, num_topics, pub_num, sub_num, dissemination, pub_id, sub_id, experiment_name) 
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

      cursor = connection.cursor()
      cursor.executemany(mySql_insert_query, self.latency_data)
      connection.commit()

      self.logger.info ("SubscriberAppln::send_latency_data_to_db - {} records inserted successfully into Latencies table".format(cursor.rowcount))
      cursor.close()

    except mysql.connector.Error as error:
      self.logger.error ("SubscriberAppln::send_latency_data_to_db - Failed to insert records into Latencies table: {}".format(error))

    finally:
      if connection.is_connected():
          connection.close()
          self.logger.info("SubscriberAppln::send_latency_data_to_db - MySQL connection is closed")

    return

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
      self.logger.info ("SubscriberAppln::handle_register_response")
      if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
        self.logger.debug ("SubscriberAppln::register_response - registration is a success")

        # set our next state to isready so that we can then send the isready message right away
        self.state = self.State.ISREADY
        
        # return a timeout of zero so that the event loop in its next iteration will immediately make
        # an upcall to us
        return 0
      
      else:
        self.logger.debug ("SubscriberAppln::register_response - registration is a failure with reason {}".format (reg_resp.reason))
        raise ValueError ("Subscriber needs to have unique id")

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
      self.logger.info ("SubscriberAppln::handle_isready_response")

      # Notice how we get that loop effect with the sleep (10)
      # by an interaction between the event loop and these
      # upcall methods.
      if not isready_resp.status:
        # discovery service is not ready yet
        self.logger.debug ("SubscriberAppln::handle_isready_response - Not ready yet; check again")
        time.sleep (10)  # sleep between calls so that we don't make excessive calls

      else:
        # we got the go ahead
        # we now look up publishers to connect to
        self.state = self.State.LOOKUP_PUBLISHERS
        
      # return timeout of 0 so event loop calls us back in the invoke_operation
      # method, where we take action based on what state we are in.
      return 0
    
    except Exception as e:
      raise e


  ########################################
  # handle isready response method called as part of upcall
  #
  # Also a part of upcall handled by application logic
  ########################################
  def handle_lookup_response (self, lookup_resp):
    ''' handle_lookup_response '''

    try:
      self.logger.info ("SubscriberAppln::handle_lookup_response")

      # connect to all publishers and subscribe to the topics we are interested in
      self.mw_obj.connect_to_publishers(lookup_resp.addressesToConnectTo)
      self.mw_obj.subscribe_to_topics(self.topiclist)
        
      self.state = self.State.RECEIVE_DATA
      
      # return standard timeout for subscription data
      # if we do not receive data for this many milliseconds, we finish application
      return self.timeout
    
    except Exception as e:
      raise e


  ########################################
  # handle receipt of subscription data 
  ########################################
  def handle_receipt_of_subscription_data (self, string_received):
    ''' handle_receipt_of_subscription_data '''

    try:
      self.logger.info ("SubscriberAppln::handle_receipt_of_subscription_data")

      self.logger.info("RECEIVED DATA: %s", string_received)
      beginning_of_payload = (string_received.find(':') + 1)
      string_received = string_received[beginning_of_payload:]

      # save latency information locally so that it can be sent to the database later
      data = ast.literal_eval(string_received)
      cur_timestamp = time.time()
      sent_timestamp = float(data['sent_timestamp'])
      latency = str(cur_timestamp - sent_timestamp)

      # INSERT INTO latencies(latency_sec, frequency, num_topics, pub_num, sub_num, pub_id, sub_id, experiment_name) VALUES ();
      self.latency_data.append((
        latency, 
        self.freq, 
        self.num_topics, 
        self.pub_num,
        self.sub_num,
        self.dissemination,
        data['pubid'], 
        self.name, 
        data['exp_name']))
      
      # return standard timeout for subscription data
      # if we do not receive data for this many milliseconds, we finish application
      return self.timeout
    
    except Exception as e:
      raise e



  ########################################
  # dump the contents of the object 
  ########################################
  def dump (self):
    ''' Pretty print '''

    try:
      self.logger.info ("**********************************")
      self.logger.info ("SubscriberAppln::dump")
      self.logger.info ("------------------------------")
      self.logger.info ("     Name: {}".format (self.name))
      self.logger.info ("     Lookup: {}".format (self.lookup))
      self.logger.info ("     Dissemination: {}".format (self.dissemination))
      self.logger.info ("     Num Topics: {}".format (self.num_topics))
      self.logger.info ("     TopicList: {}".format (self.topiclist))
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
  parser = argparse.ArgumentParser (description="Subscriber Application")

  parser.add_argument ("-n", "--name", default="sub", help="Some name assigned to us. Keep it unique per subscriber")

  parser.add_argument ("-p", "--port", type=int, default=5577, help="Port number on which our underlying subscriber ZMQ service runs, default=5577")
    
  parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")

  parser.add_argument ("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")

  parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

  parser.add_argument ("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

  parser.add_argument ("-t", "--timeout", type=int, default=20, help="Timeout for receiving subscription data. If we do not receive data for this many seconds, we assume publishers are done publishing data and we stop the application")

  # number of publishers and, frequency are added because we want to send that data to the database
  parser.add_argument("-P", "--publishers", type=int, default=2, help="Number of publishers")

  parser.add_argument("-S", "--subscribers", type=int, default=2, help="Number of subscribers")

  parser.add_argument ("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
  
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
    logger = logging.getLogger ("SubscriberAppln")
    
    # first parse the arguments
    logger.debug ("Main: parse command line arguments")
    args = parseCmdLineArgs ()

    # reset the log level to as specified
    logger.debug ("Main: resetting log level to {}".format (args.loglevel))
    logger.setLevel (args.loglevel)
    logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

    # Obtain a Subscruber application
    logger.debug ("Main: obtain the subscriber appln object")
    subscriber_app = SubscriberAppln (logger)

    # configure the object
    logger.debug ("Main: configure the subscriber appln object")
    subscriber_app.configure (args)

    # now invoke the driver program
    logger.debug ("Main: invoke the subscriber appln driver")
    subscriber_app.driver ()

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