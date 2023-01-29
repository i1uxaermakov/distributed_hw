#!/bin/sh

# ps aux | grep DiscoveryAppln.py
# ps aux | grep PublisherAppln.py
# ps aux | grep SubscriberAppln.py
# ps aux | grep BrokerAppln.py

ps aux | grep -E 'DiscoveryAppln.py|PublisherAppln.py|SubscriberAppln.py|BrokerAppln.py'