# Distributed Inference

Slower than expected :(

Performance for sending and receiving X messages using 1 client, 1 broker
and 2 workers, written in Rust:

- [Ventilator Pattern](./ventilator_rust/):
  - Rust broker: **200k msgs | 0.35s**
  - C broker: **200k msgs | 0.35s**
  - Rust broker: **1M msgs | 1.82s**
  - C broker: **1M msgs | 1.75s**

- [Majordomo Pattern (MDP)](./mdp_rust/) **50k msgs | 3.4s**
- [ROUTER-DEALER Pattern](./rd_rust/): **50k msgs | 2.5s**
