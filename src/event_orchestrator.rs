use crate::queue::Queue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct EventOrchestrator {
    pub(crate) queue_map: HashMap<String, Queue>,
}

impl EventOrchestrator {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            queue_map: Default::default(),
        }))
    }

    pub async fn queue(&mut self, queue_name: &String) {
        let contains = self.queue_map.contains_key(&queue_name.clone());
        if !contains {
            let q = Queue::new();
            self.queue_map.insert(queue_name.clone(), q);
        }
    }
}
