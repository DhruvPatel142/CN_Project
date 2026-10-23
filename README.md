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

## Tests and results

Run the Part 1 fixture checks with `python3 Results/test_part1.py`, the Part 1 throughput sweep with
`python3 Results/benchmark_part1.py`, the Case 1/Case 2 probability matrix with
`python3 Results/run_part2_failure_tests.py`, and the Enhanced probability/connection matrix and
recovery checks with `python3 Results/test_case2_enhanced.py`. The scripts use temporary
fixtures and do not alter `dataset.zip` or `raw-img/`. CSV summaries and captured server logs are in
`Results/`. Generate the PNG charts from recorded CSV data with `python3 Results/generate_plots.py`.

The implementations use a 28-byte network-order header/payload frame, exact-length socket helpers,
and raw binary file contents. This follows the Instructor Protocol's general Header/Payload message
format and its minimum header fields (message type, transaction ID, and payload length). The Team
Protocol specifies a different fixed-layout `message_header` struct. These wire encodings conflict;
the implementation follows the instructor's framing and the team's documented message names and
transfer semantics. Part 2 fault checks use 65,536-byte intervals. The Part 1 `--single` option is an
optional mode selector; the required two-argument server invocation remains the multi-threaded mode.

## Assignment naming checklist

The assignment requires the source folders to be named `Part1/`, `Part2_Case1/`,
`Part2_Case2/`, and `Part2_Case2_Enhanced/`. Run `make` inside each folder; it creates exactly
two runnable files there, named `server` and `client`. The final package should be named
`FinalSubmission_Group_040_093.zip` (or `.tar.gz`) and contain a top-level
`FinalSubmission_Group_040_093/` directory with `FinalReport_Group_040_093.pdf`, `README.md`,
the four source folders, and `Results/`. The supplied dataset is not part of the required package.
