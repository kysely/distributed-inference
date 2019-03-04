# Ventilator (Rust & C broker)

```sh
RUST_LOG=info cargo run --example client

RUST_LOG=info cargo run --example broker # Rust version
make broker && ./target/broker # C version

RUST_LOG=info cargo run --example worker
```

You can spawn multiple clients/workers using provided shells:
```sh
RUST_LOG=info ./clients 2
RUST_LOG=info ./workers 2
```

---

![Topology](topology.png)