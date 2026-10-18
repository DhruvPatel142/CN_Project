# IT305 Socket Programming Project

The project is split into four independent C implementations. Each folder has its own Makefile and
builds `server` and `client` executables locally.

## Build

Run `make clean && make` separately in `Part1/`, `Part2_Case1/`, `Part2_Case2/`, and
`Part2_Case2_Enhanced/`.

## Run

Part 1 multi-threaded server and client:

```sh
cd Part1
./server <port> <topics_root_dir>
./client <server_ip> <server_port> <topic> <output_dir>
```

For the Part 1 sequential baseline, add the optional `--single` server argument:

```sh
./server <port> <topics_root_dir> --single
```

Case 1 or Case 2 server and client (use the corresponding case folder):

```sh
cd Part2_Case1
./server <port> <topics_root_dir> <failure_probability>
./client <server_ip> <server_port> <topic> <output_dir>
```

Case 2 Enhanced:

```sh
cd Part2_Case2_Enhanced
./server <port> <topics_root_dir> <failure_probability>
./client <server_ip> <server_port> <topic> <output_dir> [num_connections]
```

The failure probability must be between `0.0` and `1.0`, inclusive. The Enhanced client defaults to
four worker connections. All clients write topic files to `<output_dir>/<topic>/`. The server discovers
topics as immediate subdirectories of the supplied root directory; no dataset paths or topic names are
embedded in the executables.

## Excluded local files

The large `raw-img/` dataset, `dataset.zip`, and generated `Results/` artifacts are kept out of this repository. Supply your own extracted topic folders when starting a server. The local test scripts and measurement outputs are not included.

The implementations use a 28-byte network-order header/payload frame, exact-length socket helpers,
and raw binary file contents. This follows the Instructor Protocol's general Header/Payload message
format and its minimum header fields (message type, transaction ID, and payload length). The Team
Protocol specifies a different fixed-layout `message_header` struct. These wire encodings conflict;
the implementation follows the instructor's framing and the team's documented message names and
transfer semantics. Part 2 fault checks use 65,536-byte intervals. The Part 1 `--single` option is an
optional mode selector; the required two-argument server invocation remains the multi-threaded mode.
