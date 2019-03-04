# Distributed Inference

Slow :( The broker is the biggest bottleneck in all patterns. It might
be useful to try writing it in C/C++, using vanilla ZMQ, without bindings.

Performance for sending and receiving X messages using 1 client, 1 broker
and 2 workers, written in Rust:

- [Ventilator Pattern](./ventilator_rust/): **200k msgs | 1.8s**
- [Majordomo Pattern (MDP)](./mdp_rust/) **50k msgs | 3.4s**
- [ROUTER-DEALER Pattern](./rd_rust/): **50k msgs | 2.5s**