# Majordomo Protocol

Slightly changed framing and headers.

It originated from the [Majordomo Protocol](https://rfc.zeromq.org/spec:7/MDP)
defined in [Chapter 4 of ZeroMQ Guide](http://zguide.zeromq.org/page:all#toc86)
by Pieter Hintjens.

---


## Message Types
**Each message** contains a valid header consisting of following **parts**:
1) Name and version of the protocol (4 bytes)
2) Command (1 byte)
3) Route, _optional if needed by command_ (any length)

All header parts MUST be separated by `\n` byte.

### Data Message
Data message consists of **minimum 3 frames**, formatted as follows:
1) three-part header (third part, router, is the name of target service)
2) message ID (any length)
3) data frame (any length)
4) ... arbitrary following data frames

Valid data message commands:
- `REQUEST` (`0x01`)
- `REPLY` (`0x02`)


### Control Message
Control message consists of **exactly 1 frame**, the header.

Valid control message commands:
- `READY` (`0x03`), has to specify service worer service name in third part of header
- `PING` (`0x04`)
- `PONG` (`0x05`)
- `DISCONNECT` (`0x06`)

### Example

Routing request (`%x1`) to service named `echo`. Header delimiter shown as `\`.

```
  CLIENT OUT
+---------------+----+------+
| DI10\%x1\echo | ID | DATA |
+---------------+----+------+

  BROKER IN
+--------+---------------+----+------+
| C_ADDR | DI10\%x1\echo | ID | DATA |
+--------+---------------+----+------+

  BROKER OUT
+--------+-----------------+----+------+
| W_ADDR | DI10\%x1\C_ADDR | ID | DATA |
+--------+-----------------+----+------+
         ^------- goes on wire --------^

  WORKER IN
+-----------------+----+------+
| DI10\%x1\C_ADDR | ID | DATA |
+-----------------+----+------+

  (and vice versa)
```
