# ROUTER-DEALER pattern

Basically [this](http://zeromq.org/sandbox:dealer), only using `DEALER` socket for both clients and workers.

![Topology](http://zeromq.org/local--files/sandbox:dealer/fig1.png)

DEALER in broker acts as a round-robin load balancer.