# Part 2, Case 1: Restart whole topic on failure

Build with `make`. Run the server and client using the required interfaces:

```sh
./server <port> <topics_root_dir> <failure_probability>
./client <server_ip> <server_port> <topic> <output_dir>
```

The failure probability is a floating-point value from `0.0` through `1.0`. The server prints its configured probability and check interval at startup and logs every injected failure, including a cumulative event number, topic, file, and offset. A decision is made after each 64 KiB of acknowledged file data for the connection; it is not made per `send()` call.

The client discards an unsuccessful attempt's staging files and reconnects with a fresh GET. It repeats the complete topic transfer and publishes the staged topic files only after CLOSE. An unknown-topic/application error is not retried.

The message framing and file listing come from Part 1. Case 1 adds server-side fault injection and full-topic restart behavior; it keeps no transfer session state.
