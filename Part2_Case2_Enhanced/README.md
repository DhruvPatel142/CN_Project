# Part 2 — Case 2 Enhanced

This folder implements file-level parallel, checkpointed topic transfer over TCP. It is independent of the Part 1, Case 1, and basic Case 2 folders.

## Build and run

From this folder:

```sh
make clean
make
./server <port> <topics_root_dir> <failure_probability>
./client <server_ip> <server_port> <topic> <output_dir> [num_connections]
```

The client defaults to 4 worker connections. The optional count accepts 1–64; the required experiment values 1, 2, 4, and 8 are supported. The server probability accepts finite values from 0.0 through 1.0. Example:

```sh
./server 5000 ../raw-img 0.10
./client 127.0.0.1 5000 elefante ./received 4
```

The client writes each file under `<output_dir>/<topic>/`. Topics are immediate subdirectories of the supplied server root. Only regular files directly inside a topic are listed.

## Protocol and recovery

Every control message uses the existing common 28-byte network-order frame: zero-padded type, 64-bit transaction ID, 32-bit payload size, and payload. File bytes are binary raw bytes following the `FILE` metadata and are read/written with exact-length helpers.

1. Control connection: `GET` (topic) → `SESSION` (session ID and file count) → one `FILELIST_ENTRY` per file → `FILELIST_END`. The control connection then closes.
2. Each worker connection repeatedly requests one whole file using `FILE_REQUEST` (session ID, file index, offset). The server rejects invalid sessions, indices, offsets, completed files, and duplicate in-progress assignments.
3. The server returns `FILE` metadata and streams that file from the accepted offset. The client flushes each 64 KiB checkpoint to disk before sending the documented `CHECKPOINT` message. The server validates and saves it before continuing; the client treats the flushed and successfully reported offset as its last confirmed checkpoint. A final short checkpoint is sent at EOF, but it does not trigger a failure-probability draw unless a full 64 KiB interval has elapsed.
4. The client sends `FILE_DONE` after receiving the complete file; the server confirms it with `FILE_DONE`. The worker then takes the next index from the client’s mutex-protected shared queue. When all workers finish, the client sends `SESSION_DONE` on the control path and the server frees the session.
5. If a worker connection fails, only its current file is retried. The client truncates the temporary file to its last locally flushed and reported checkpoint, reconnects, and requests that file from the confirmed offset. Other workers continue. A lost completion response is safe to retry: a completed file request at its full size is accepted so the server can repeat the completion confirmation.

The server owns a mutex-protected session table with per-file completion, current owner, and confirmed offset. Idle sessions expire after one hour using a periodic reaper. An injected failure is checked server-side after each 64 KiB of transferred file data and confirmed checkpoint, never after each individual `send()`. File-level scheduling does not split files into byte ranges.

The implementation uses the project’s shared framing and network-byte-order helpers, thread-per-worker model, sorted regular-file listing, and checkpointed-resume approach. `CHECKPOINT_OK` is not added as a separate message type; the client reports the checkpoint using the documented message, and the server validates it before proceeding. The server echoes `FILE_DONE` with the same documented method after marking the file complete so the client can count a confirmed completion.

## Logs and measurements

The server prints a startup `CONFIG` line with the configured probability and interval, `SESSION_CREATED`/`SESSION_DONE` lifecycle messages, and one `FAULT_INJECTED` line per actual injected failure with session, file, offset, probability, and interval. The client prints file completion records plus total file bytes received, useful and redundant bytes, checkpoint count and outbound framing overhead (44 bytes per 28-byte frame plus 16-byte checkpoint payload), retries, reconnects, duration, and useful-byte throughput.

## Test artifacts

Run the local integration suite from the project root with:

```sh
python3 Results/test_case2_enhanced.py
```

It exercises probabilities 0.0, 0.05, 0.10, and 0.20 at 1, 2, 4, and 8 workers, verifies a staged sample copied from the provided dataset byte-for-byte, forces a separate probability-1.0 recovery run, checks empty and invalid topics, and verifies a full supplied dataset topic at zero failure probability. The CSV files and server logs are under `Results/`. Observed failure counts are random finite-run results, not exact percentages.

## Portability

The code uses the existing Linux/macOS/Windows socket abstraction, POSIX threads on Linux/macOS, and Windows threads on Windows. Builds and tests in this workspace were performed on Linux; Windows and macOS builds were not run here.

## Prototype review

The workspace contains the Instructor Protocol and Team Protocol PDFs, but no separate prototype source-code files. Their wire-format difference is recorded in the root README: this implementation uses the instructor reference's Header/Payload framing and follows the Team Protocol's message names and transfer semantics. The project code uses sorted regular-file listing, validated file sizes, exact-length send/receive helpers, dynamic topic paths, confirmed checkpoints, a shared file queue, per-file ownership, parallel workers, and per-worker recovery.
