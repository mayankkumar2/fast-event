#![feature(future_join)]

use event_orchestrator::EventOrchestrator;
use std::error::Error;
use std::future::join;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task;

mod errors;
mod event_orchestrator;
mod handlers;
mod queue;
mod stream_processor;

async fn send_listener(
    listener: TcpListener,
    orchestrator: Arc<Mutex<EventOrchestrator>>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("[PUBLISH] Listening...");
    loop {
        let (socket, sock_addr) = listener.accept().await?;
        let orchestrator = orchestrator.clone();
        task::spawn(handlers::sender_socket_handler(
            orchestrator,
            socket,
            sock_addr,
        ));
    }
}

async fn receiver_listener(
    listener: TcpListener,
    orchestrator: Arc<Mutex<EventOrchestrator>>,
) -> Result<(), Box<dyn Error>> {
    println!("[SUBSCRIBER] Listening...");
    loop {
        let (socket, sock_addr) = listener.accept().await?;
        let orchestrator = orchestrator.clone();
        task::spawn(handlers::receiver_socket_handler(
            orchestrator,
            socket,
            sock_addr,
        ));
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let sender_listener_tcp = TcpListener::bind("0.0.0.0:8161").await?;

    let receiver_listener_tcp = TcpListener::bind("0.0.0.0:8162").await?;

    let orchestrator = EventOrchestrator::new();

    let sender_orchestrator = orchestrator.clone();
    let receiver_orchestrator = orchestrator.clone();

    let sender_handler = send_listener(sender_listener_tcp, sender_orchestrator);

    let receiver_handler = receiver_listener(receiver_listener_tcp, receiver_orchestrator);

    let _ = join!(sender_handler, receiver_handler).await;
    Ok(())
}
