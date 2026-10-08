//! Follow one order from the command line: a native partyline client on tokio.
//!
//! ```shell
//! cargo run -p orders-tail -- http://localhost:8787 order-1
//! cargo run -p orders-tail -- http://localhost:8787 order-1 4503599627370495.12
//! ```
//!
//! It prints every event and every status change. Without a cursor it receives live events
//! only. With a cursor it first receives every retained event after it, or a reset.

#[cfg(not(target_arch = "wasm32"))]
mod native;

#[cfg(not(target_arch = "wasm32"))]
fn main() -> std::process::ExitCode {
    native::main()
}

/// The example is native only. This stub keeps workspace-wide wasm32 builds working.
#[cfg(target_arch = "wasm32")]
fn main() {}
