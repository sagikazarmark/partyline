# orders-tail

A native command-line client for the [orders example](..). It follows one order and prints every event and every status change.
It shows `partyline-client` on tokio, with the native transport, outside Dioxus.

## Run it

Start the orders example first, for example with `just examples dev orders`. Then:

```shell
cargo run -p orders-tail -- http://localhost:8787 demo
```

| Argument | Meaning |
| --- | --- |
| Base URL | The Worker's origin. `http` and `https` become `ws` and `wss` |
| Order ID | The channel ID. The web client shows the order `demo` |
| Cursor | Optional. `{epoch}.{seq}`: resume after this event. Without it, the client receives live events only |

Change the status in the browser. The terminal prints lines like these:

```text
status Connecting
status Open
event  4503599627370495.1 StatusChanged { status: Preparing }
event  4503599627370495.2 NoteAdded { note: "No onions" }
```

Stop the Worker and start it again to see the reconnect: `Waiting { .. }`, then `Open`.
Press Ctrl-C to close the socket with 1000. The client prints its last cursor; pass it as the third argument to resume from there.
It exits with status 0 after Ctrl-C, and 1 when the client stops for any other reason, such as a terminal close code or an event it cannot decode.

Status lines are printed with `{:?}`, so they show every field of `Status`, such as the attempt number and the last close code.
