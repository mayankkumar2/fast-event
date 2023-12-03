use crate::errors::FragmentationProcessorError;
use crate::event_orchestrator::EventOrchestrator;
use crate::queue::{Message, Signal};
use crate::stream_processor::StreamProcessor;
use std::net::SocketAddr;
use std::sync::{Arc};
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::broadcast::Receiver;
use tokio::sync::RwLock;

pub async fn receiver_socket_handler(
    event_orchestrator: Arc<RwLock<EventOrchestrator>>,
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

    println!("Handling request for SUBSCRIBER [{}]", queue_name);

    {
        event_orchestrator.write().await.queue(&queue_name, 0xfffffffff);
    }

    let mut _read_idx = 0u128;
    let mut messages: Vec<Arc<Message>> = Vec::new();
    let mut sig_wait: Receiver<Signal>;
    {
        let orc = event_orchestrator.read().await;
        let q = orc.queue_map.get(&queue_name).unwrap();
        sig_wait = q.signal_recv();
        _read_idx = q.start_idx;
    }

    {
        event_orchestrator.write().await.seek(&queue_name, _read_idx, _read_idx);
    }

    // INITIAL CHECKS
    {
        let old_seek = _read_idx;
        {
            let orc = event_orchestrator.read().await;
            let q = orc.queue_map.get(&queue_name).unwrap();
            let msg = q.poll_message(&mut _read_idx);
            if let Ok(msg) = msg {
                messages = msg;
            } else {
                println!("{:?}", msg.err());
            }
        }

        if messages.len() > 0 {
            event_orchestrator.write().await.seek(&queue_name, old_seek, _read_idx);
        }

        println!("[RECEIVER {} messages..]", messages.len());
        for message in messages {
            let r = frame_provider.1.send(message.message.to_string()).await;
            if r.is_err() {
                eprintln!("error: {:?}", r.err());
                return;
            }
        }
    }




    loop {
        select! {
            _ = sig_wait.recv() => {
                {
                    let old_seek = _read_idx;
                    {
                        let orc = event_orchestrator.read().await;
                        let q = orc.queue_map.get(&queue_name).unwrap();
                        let msg = q.poll_message(&mut _read_idx);
                        if let Ok(msg) = msg {
                            messages = msg;
                        } else {
                            println!("{:?}", msg.err());
                            continue;
                        }
                    }

                    if messages.len() > 0 {
                        event_orchestrator.write().await.seek(&queue_name, old_seek, _read_idx);
                    }

                    println!("[RECEIVER {} messages..]", messages.len());
                    for message in messages {
                        let r = frame_provider.1.send(message.message.to_string()).await;
                        if r.is_err() {
                            eprintln!("error: {:?}", r.err());
                            return;
                        }
                    }
                }
            },
            x = frame_provider.0.recv() => {
                let v = x.unwrap();
                if v.is_err() && v.err().unwrap() == FragmentationProcessorError::ConnectionClosed {
                    {
                        event_orchestrator.write().await.down(&queue_name, _read_idx);
                    }
                    return;
                }
            }
        }
    }
}

pub async fn sender_socket_handler(
    event_orchestrator: Arc<RwLock<EventOrchestrator>>,
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

    println!("Handling request for PUBLISHER [{}]", queue_name);

    {
        event_orchestrator.write().await.queue(&queue_name, 0xfffffffff);
    }

    loop {
        let frame = frame_provider.0.recv().await.unwrap();

        if let Ok(frame) = frame {
            let mut orc = event_orchestrator.write().await;
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
