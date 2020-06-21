from mininet.node import CPULimitedHost
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.log import setLogLevel, info
from mininet.node import RemoteController
from mininet.cli import CLI


class DDOS_Topo(Topo):

    def __init__(self , **opts):

        # Initialize topology
        super(DDOS_Topo, self).__init__(**opts)

        # Add hosts and switches
        s1 = self.addSwitch( 's1' )
        s2 = self.addSwitch( 's2' )
        self.addLink(s1, s2)
        n = 5

        for number in range(n):
            target = self.addHost( 'target{}'.format(number + 1), cpu = 0.4/2*n, ip='10.0.0.{}'.format(number + 1), mac='00:00:00:00:00:0{}'.format(number + 1))
            self.addLink(target, s1)
        
        attacker = self.addHost( 'attacker1', cpu = 0.15, ip='10.0.1.1')     
        self.addLink(attacker, s2) 
        attacker = self.addHost( 'attacker2', cpu = 0.15, ip='10.0.1.2')     
        self.addLink(attacker, s2) 

TOPOS = {'DDOS_Topo' : (lambda : DDOS_Topo())}

def run():
    net = Mininet(topo=DDOS_Topo(), host=CPULimitedHost)
    net.addController('c0', cpu=0.3)
    net.start()
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
