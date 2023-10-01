use crate::errors::{FragmentationProcessorError, OffsetErr};
use std::collections::VecDeque;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};

pub struct Queue {
    queue: VecDeque<Arc<Message>>,
    start_idx: u128,
    pub(crate) end_idx: u128,
    inbound_sink: Sender<Signal>,
}

impl Queue {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel::<Signal>(256);

        Self {
            queue: VecDeque::new(),
            start_idx: 0,
            end_idx: 0,
            inbound_sink: tx,
        }
    }

    pub fn send_message(&mut self, message: Message) -> Result<(), Box<dyn Error>> {
        let msg = Arc::new(message);
        self.queue.push_back(msg);
        self.end_idx += 1;
        self.inbound_sink.send(Signal)?;
        Ok(())
    }

    pub fn signal_recv(&self) -> Receiver<Signal> {
        return self.inbound_sink.subscribe();
    }

    pub fn poll_message(&self, read_idx: &mut u128) -> Result<Vec<Arc<Message>>, OffsetErr> {
        if *read_idx > self.end_idx && *read_idx < self.start_idx {
            return Err(OffsetErr);
        }

        println!("IDX: {}: {}", read_idx, self.end_idx);


        let mut messages = Vec::new();

        while *read_idx < self.end_idx {
            let offset = *read_idx - self.start_idx;
            let message = self.queue.get(offset as usize).unwrap().clone();
            messages.push(message);
            (*read_idx) += 1;
        }

        return Ok(messages);
    }
}

pub struct Message {
    pub message: Box<str>,
}

#[derive(Clone, Debug)]
pub struct Signal;

pub fn deserialize(s: &String) -> &[u8] {
    return s.as_bytes();
}


// return len till which we have read to remove it from the buffer
pub fn serialize(bytes: &[u8]) -> (Result<String, FragmentationProcessorError>, usize) {
    let mut seek = 0usize;
    for byte in bytes {
        seek += 1;
        let ch = char::from(*byte);
        if ch == '\n' {
            let b = String::from_utf8_lossy(&bytes[0..(seek - 1)]).to_string();
            return (Ok(b), seek);
        }
    }
    return (Err(FragmentationProcessorError::UnexpectedError), 0);
}
