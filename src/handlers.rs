use crate::errors::FragmentationProcessorError;
use crate::event_orchestrator::EventOrchestrator;
use crate::queue::{Message, Signal};
use crate::stream_processor::StreamProcessor;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;

pub async fn receiver_socket_handler(
    event_orchestrator: Arc<Mutex<EventOrchestrator>>,
    socket: TcpStream,
    sock_addr: SocketAddr,
) {
    println!("Handling [SUBSCRIBER] request from [{:?}]", sock_addr);

    let stream_processor = StreamProcessor {
        fragmentation_processor: crate::queue::serialize,
        stream: socket,
        frame_deserialization: crate::queue::deserialize,
    };

    let mut frame_provider = stream_processor.frame_provider();

    let queue_name = match frame_provider.0.recv().await.unwrap() {
        Ok(queue_name) => queue_name,
        Err(_) => {
            return;
        }
    };

    {
        event_orchestrator.lock().await.queue(&queue_name).await;
    }

    let mut _read_idx = 0u128;
    let mut message: Arc<Message>;
    let mut sig_wait: Receiver<Signal>;
    {
        let orc = event_orchestrator.lock().await;
        let q = orc.queue_map.get(&queue_name).unwrap();
        sig_wait = q.signal_recv();
        _read_idx = q.end_idx;
    }

    loop {
        select! {
            _ = sig_wait.recv() => {
                {
                    {
                        let mut orc = event_orchestrator.lock().await;
                        let q = orc.queue_map.get_mut(&queue_name).unwrap();
                        let msg = q.poll_message(_read_idx);
                        _read_idx += 1;
                        if let Ok(msg) = msg {
                            message = msg;
                        } else {
                            println!("{:?}", msg.err());
                            continue;
                        }
                    }
                    println!("[RECEIVER signal: {}]", message.message);
                    let r = frame_provider.1.send(message.message.to_string()).await;
                    if r.is_err() {
                        eprintln!("error: {:?}", r.err());
                        return;
                    }

                }
            },
            x = frame_provider.0.recv() => {
                let v = x.unwrap();
                if v.is_err() && v.err().unwrap() == FragmentationProcessorError::ConnectionClosed {
                    return;
                }
            }
        };
    }
}

pub async fn sender_socket_handler(
    event_orchestrator: Arc<Mutex<EventOrchestrator>>,
    socket: TcpStream,
    sock_addr: SocketAddr,
) {
    println!("Handling PUBLISHER request from [{:?}]", sock_addr);

    let stream_processor = StreamProcessor {
        fragmentation_processor: crate::queue::serialize,
        stream: socket,
        frame_deserialization: crate::queue::deserialize,
    };

    let mut frame_provider = stream_processor.frame_provider();

    let queue_name = match frame_provider.0.recv().await.unwrap() {
        Ok(queue_name) => queue_name,
        Err(_) => {
            return;
        }
    };

    {
        event_orchestrator.lock().await.queue(&queue_name).await;
    }

    loop {
        let frame = frame_provider.0.recv().await.unwrap();

        if let Ok(frame) = frame {
            let mut orc = event_orchestrator.lock().await;
            let q = orc.queue_map.get_mut(&queue_name).unwrap();
            println!("[CONTENT] {:?}", frame);
            q.send_message(Message {
                message: Box::from(frame),
            })
            .unwrap();
        } else {
            if frame.err().unwrap() == FragmentationProcessorError::ConnectionClosed {
                return;
            }
        }
    }
}
