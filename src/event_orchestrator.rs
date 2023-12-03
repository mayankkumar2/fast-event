use crate::queue::Queue;
use std::collections::HashMap;
use std::sync::{Arc};
use tokio::sync::RwLock;

pub struct EventOrchestrator {
    pub(crate) queue_map: HashMap<String, Queue>,
    idx_reader_map: HashMap<String, HashMap<u128, i128>>
}

impl EventOrchestrator {
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            queue_map: Default::default(),
            idx_reader_map: HashMap::new()
        }))
    }

    pub fn queue(&mut self, queue_name: &String, seek: u128) {
        let mut seek = seek;
        let contains = self.queue_map.contains_key(&queue_name.clone());
        if !contains {
            let q = Queue::new();
            self.queue_map.insert(queue_name.clone(), q);
            if !self.idx_reader_map.contains_key(queue_name) {
                self.idx_reader_map.insert(queue_name.clone(), HashMap::new());
            }
        } else {
            let q = self.queue_map.get(queue_name).unwrap();
            if seek < q.start_idx && seek > q.end_idx {
                seek = q.end_idx;
            }
        }
        let reader_map = self.idx_reader_map
            .get_mut(queue_name)
            .unwrap();

        if !reader_map.contains_key(&seek) {
            reader_map.insert(seek, 1);
        } else {
            let value = reader_map[&seek];
            reader_map.insert(seek, value + 1);
        }
    }

    pub fn seek(&mut self, queue_name: &String, old_seek: u128, seek: u128) {
        let contains = self.queue_map.contains_key(&queue_name.clone());
        if contains {
            let reader_map = self.idx_reader_map
                .get_mut(queue_name)
                .unwrap();
            if let Some(value) = reader_map.get(&seek) {
                HashMap::insert(reader_map, seek, value + 1);
            } else {
                reader_map.insert(seek, 1);
            }
            if reader_map.contains_key(&old_seek) {
                let value = reader_map[&old_seek];
                reader_map.insert(old_seek, value - 1);
            }
        }
    }

    pub fn down(&mut self, queue_name: &String, seek: u128) {
        let contains = self.queue_map.contains_key(&queue_name.clone());
        if contains {
            let reader_map = self.idx_reader_map
                .get_mut(queue_name)
                .unwrap();
            if reader_map.contains_key(&seek) {
                let value = reader_map[&seek];
                reader_map.insert(seek, value - 1);
            }
        }
    }

    pub async fn cleaner(&mut self) {
        for (queue_name, queue) in self.queue_map.iter_mut() {
            let readers = self.idx_reader_map.get_mut(queue_name);
            if let Some(q) = readers {
                loop {
                    if let Some(count) = q.get(&queue.start_idx) {
                        if *count <= 0i128 {
                            println!("Cleaning Index {}" , &queue.start_idx);
                            q.remove(&queue.start_idx);
                            queue.clean();
                        } else {
                            break;
                        }
                    } else {
                        println!("Cleaning Index {}" , &queue.start_idx);
                        queue.clean();
                    }
                    if queue.start_idx == queue.end_idx {
                        break;
                    }
                }
            }
        }
    }

}
