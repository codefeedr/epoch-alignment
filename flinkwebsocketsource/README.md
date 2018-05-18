# Flink Websocket Source

A simple flink sourcefunction, which connects to a websocket and exposes the data as a stream.

The client (the flink source) sends an offset and a number of messages. The server responds with the number of messages that was requested by the client. 

## Usage

TODO

## Automatic shutdown

If configured, the source will terminate if it starts recieving empty messages. If the Flink job runs in checkpointed mode, the source will wait at least until the last checkpoint with a non-empty message + 1 until it terminates (This will guarantee the non-empty messages have been processed).
