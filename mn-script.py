# Credit to Ben Van Sleen for the initial script
# The file has been changed from what it used to be

from argparse import ArgumentParser
from time import sleep
from random import randint
from mininet.net import Mininet
from mininet.node import Node
from mininet.topolib import TreeTopo
from mininet.log import setLogLevel, info
from mininet.clean import cleanup
import sys    # for syspath and system exception

class MininetRunner():

    ########################################
    # constructor
    ########################################
    def __init__ (self, pubs, subs, freq, n_topics, has_broker):
        self.pubs = pubs # number of publishers
        self.subs = subs # number of subscribers
        self.freq = freq # frequency of dissemination
        self.topics = n_topics # number of topics
        self.has_broker = has_broker # boolean variable that says wether we have a broker
        self.experiment_name = "P" + str(self.pubs) + \
            "S" + str(self.subs) + \
            "F" + str(self.freq) + \
            "T" + str(self.topics) + \
            "D" + ("B" if self.has_broker else "D") # for dissemination
        self.net = None # mininet tree


    def set_up_net_and_start(self, depth: int, fanout: int, **kwargs):
        self.net = Mininet(
            TreeTopo(depth=depth, fanout=fanout),
            waitConnected=True,
            **kwargs
        )
        self.net.addNAT().configDefault()

        self.net.start()


    def ifconfig(self):
        for i, host in enumerate(self.net.hosts):
            host.cmd(f'ifconfig h{i+1}-eth0 10.0.0.{i+1}')
        return


    def discovery(self, i: int) -> str:
        port = 5555
        addr = f'"10.0.0.{i+1}"'
        return f'python3 DiscoveryAppln.py -P {self.pubs} -S {self.subs} > ./logs/{self.experiment_name}_log_discovery.out 2>&1 &', f'{addr}:{port}'



    def publisher(self, i, discovery) -> str:
        pub_name = 'pub' + str(i-1)
        s = 'python3 PublisherAppln.py '
        s += f'-a "10.0.0.{i+1}" '
        s += f'-p {randint(2000, 6000)} '
        s += f'-d {discovery} '
        s += f'-T {self.topics} '
        s += f'-f {self.freq} '
        s += f'-n {pub_name} '
        s += f'-en {self.experiment_name} '
        s += f" > ./logs/{self.experiment_name}_log_{pub_name}.out 2>&1 &"
        return s


    def subscriber(self, i, discovery) -> str:
        sub_name = 'sub' + str(i+1-(self.pubs+2))
        s = 'python3 SubscriberAppln.py '
        s += f'-p {randint(2000, 6000)} '
        s += f'-d {discovery} '
        s += f'-T {self.topics} '
        s += f'-n {sub_name} '
        s += f'-P {self.pubs} '
        s += f'-S {self.subs} '
        s += f'-f {self.freq} '
        s += f" > ./logs/{self.experiment_name}_log_{sub_name}.out 2>&1 &"
        return s


    def broker(self, i, discovery) -> str:
        s = 'python3 BrokerAppln.py '
        s += f'-a "10.0.0.{i+1}" '
        s += f'-p {randint(2000, 6000)} '
        s += f'-d {discovery} '
        s += f'-n broker1 '
        s += f" > ./logs/{self.experiment_name}_log_broker1.out 2>&1 &"
        return s


    def launch(self):
        disc, discovery_ipport = self.discovery(0)
        self.net.hosts[0].sendCmd(disc)

        if (self.has_broker):
            self.net.hosts[1].sendCmd(self.broker(1, discovery_ipport))

        for i in range(2, self.pubs+2):
            self.net.hosts[i].sendCmd(
                self.publisher(i, discovery_ipport)
            )

        for i in range(self.pubs+2, self.pubs+self.subs+1):
            self.net.hosts[i].sendCmd(
                self.subscriber(i, discovery_ipport)
            )

        last_idx = self.pubs + self.subs + 2
        self.net.hosts[last_idx].cmdPrint(
            self.subscriber(last_idx, discovery_ipport)
        )
        return

    def stop_net(self):
        self.net.stop()


# def parse_args() -> ArgumentParser:
#     parser = ArgumentParser()
#     parser.add_argument('-p', '--pubs', type=int, default=10)
#     parser.add_argument('-s', '--subs', type=int, default=10)
#     parser.add_argument('-t', '--num_topics', type=int, default=9)
#     parser.add_argument('-f', '--frequency', type=int, default=1)
#     parser.add_argument('-d', '--dissemination', default='direct')
#     return parser.parse_args()


if __name__ == '__main__':
    # args = parse_args()
    setLogLevel('info')

    running_with_broker = False
    analyzed_subs_nums = [5]
    analyzed_pubs_nums = [5]
    analyzed_frequency = [30]
    anaylyzed_num_topics = [1]

    # analyzed_frequency = [60, 120]
    # anaylyzed_num_topics = [5, 9]

    # analyzed_subs_nums = [10]
    # analyzed_pubs_nums = [10]
    # analyzed_frequency = [120]
    # anaylyzed_num_topics = [9]

    # 10 10 60 5

    for sub_num in analyzed_subs_nums:
        for pub_num in analyzed_pubs_nums:
            for freq in analyzed_frequency:
                for num_topics in anaylyzed_num_topics:
                    

                    mininet_runner = MininetRunner(pub_num, sub_num, freq, num_topics, running_with_broker)
                    
                    try:
                        mininet_runner.set_up_net_and_start(depth=3, fanout=3)
                        mininet_runner.ifconfig()
                        mininet_runner.launch()

                        # waiting for messages to be sent and for the timeout
                        time_to_wait = 100 / freq * 60 + 30
                        sleep(time_to_wait)

                        mininet_runner.stop_net()
                        
                    except Exception as e:
                        if(not isinstance(e, AssertionError)):
                            cleanup()
                            type, value, traceback = sys.exc_info()
                            print(f"Type: {type}")
                            print(f"Value: {value}")
                            print(f"Traceback: {traceback.format_exc()}")
                        
                    # cleanup anyway after running everything
                    cleanup()

                    print(f"Done with sub_num {sub_num}, pub_num {pub_num}, freq {freq}, num_topics {num_topics}")
