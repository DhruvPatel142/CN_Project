# Part 1: Topic-based TCP file distribution

## Build and run

```sh
make
./server <port> <topics_root_dir>
./client <server_ip> <server_port> <topic> <output_dir>
```

The default server is multi-threaded, with one worker per client. Run the sequential baseline with:

```sh
./server <port> <topics_root_dir> --single
```

Without a configuration file, each immediate subdirectory of `topics_root_dir` is a topic. An optional `topics.conf` in that root can instead map topics to directories, one `topic=directory` pair per line. Relative directory values are resolved from `topics_root_dir`; absolute values are accepted. Blank lines and lines beginning with `#` are ignored. For example:

```text
cane=raw-img/cane
cats=/srv/animals/cats
```

The client writes the received files under `<output_dir>/<topic>/`.

## Part 1 protocol

Each TCP message has a fixed 28-byte network-order header containing a zero-padded message type, a 64-bit transaction ID, and a 32-bit payload length. The receiver reads the complete header and payload with `recv_all`; the sender uses `send_all` for each complete frame.

The exchange is `GET` → `OK` (topic and ordered file list) → `START` → `FILE_HDR` → `ACK` → repeated `DATA` / `ACK` chunks → `CLOSE`. `ERR` reports invalid requests, unknown topics, and filesystem errors. File bytes stay binary and each DATA payload includes the topic, filename, offset, and chunk length. Files are enumerated in sorted name order and only regular files are sent.
