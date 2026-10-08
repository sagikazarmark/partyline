//! The client, on tokio.

use std::process::ExitCode;

use futures::StreamExt;
use orders_shared::Orders;
use partyline::Cursor;
use partyline_client::{BaseUrl, ClientEvent, ConnectOptions};

const USAGE: &str = "usage: orders-tail <base-url> <order-id> [cursor]";

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (base_url, id, since) = match args.as_slice() {
        [base_url, id] => (base_url, id, None),
        [base_url, id, cursor] => match cursor.parse::<Cursor>() {
            Ok(cursor) => (base_url, id, Some(cursor)),
            Err(e) => {
                eprintln!("invalid cursor {cursor:?}: {e}\n{USAGE}");
                return ExitCode::FAILURE;
            }
        },
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    let (handle, mut events, driver) = partyline_client::connect::<Orders>(ConnectOptions {
        since,
        ..ConnectOptions::new(BaseUrl::Explicit(base_url.clone()), id.clone())
    });
    let driver = tokio::spawn(driver);

    // Ctrl-C closes the socket with 1000. The driver then ends, and so does the stream.
    let stop = handle.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            stop.stop();
        }
    });

    while let Some(message) = events.next().await {
        match message {
            ClientEvent::Event { epoch, seq, event } => println!("event  {epoch}.{seq} {event:?}"),
            ClientEvent::Reset => {
                println!("reset  the cursor cannot be resumed: refetch the order")
            }
            // Debug output, so the line keeps working as the Status type grows.
            ClientEvent::Status(status) => println!("status {status:?}"),
        }
    }

    let _ = driver.await;
    match handle.cursor() {
        Some(cursor) => println!("cursor {cursor}"),
        None => println!("cursor none"),
    }
    ExitCode::SUCCESS
}
