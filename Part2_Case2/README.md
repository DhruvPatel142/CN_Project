# Part 2, Case 2: Checkpointed resume

Build with `make`. The command-line interface is:

```sh
./server <port> <topics_root_dir> <failure_probability>
./client <server_ip> <server_port> <topic> <output_dir>
```

The probability accepts finite floating-point values from `0.0` to `1.0`, inclusive. Server logs report the configured value and 64 KiB check interval; each injected failure is logged with a cumulative event number and the confirmed file offset. Decisions happen after a DATA chunk has been received, written, checkpointed by the client, and acknowledged by the server.

The server assigns a session ID and stores the sorted file list for the session. The client keeps the session ID, confirmed offsets, and partial files during reconnection. After each DATA chunk is flushed locally, the client sends CHECKPOINT with the session ID, file index, and byte offset. On a broken connection, it sends RESUME with its last locally confirmed offset; the server validates the session, file index, and file-size bound, then continues from that byte. Checkpoint and resume use only the documented message types; the server does not add an undocumented checkpoint-ack message. Case 2 uses one thread per client and does not include Case 2 Enhanced features.
