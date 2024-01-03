use std::collections::HashMap;

pub trait Replaceable {
    fn victim(&mut self) -> Option<usize>;
    fn pin(&mut self, frame_id: usize);
    fn unpin(&mut self, frame_id: usize);
}

pub enum Replacer {
    LRU(LRUReplacer),
}
impl Replacer {
    pub fn victim(&mut self) -> Option<usize> {
        match self {
            Self::LRU(replacer) => replacer.victim(),
        }
    }
    pub fn pin(&mut self, frame_id: usize) {
        match self {
            Self::LRU(replacer) => replacer.pin(frame_id),
        }
    }
    pub fn unpin(&mut self, frame_id: usize) {
        match self {
            Self::LRU(replacer) => replacer.unpin(frame_id),
        }
    }
}

pub struct LRUReplacer {
    frame_map: HashMap<usize, u128>,
    counter: u128,
}
impl LRUReplacer {
    pub fn new() -> Self {
        Self {
            frame_map: HashMap::new(),
            counter: 0,
        }
    }
}
impl Replaceable for LRUReplacer {
    fn victim(&mut self) -> Option<usize> {
        if self.frame_map.is_empty() {
            return None;
        }
        let mut min_frame_id = 0;
        let mut min_counter = u128::MAX;
        for (&frame_id, &counter) in self.frame_map.iter() {
            if counter < min_counter {
                min_frame_id = frame_id;
                min_counter = counter;
            }
        }
        self.frame_map.remove(&min_frame_id);
        Some(min_frame_id)
    }
    fn pin(&mut self, frame_id: usize) {
        self.frame_map.remove(&frame_id);
    }
    fn unpin(&mut self, frame_id: usize) {
        self.frame_map.insert(frame_id, self.counter);
        self.counter = self.counter.wrapping_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_replacer() {
        let mut replacer = Replacer::LRU(LRUReplacer::new());

        assert_eq!(replacer.victim(), None);
        replacer.pin(1);
        replacer.pin(2);
        replacer.pin(3);
        replacer.pin(4);
        replacer.pin(2);
        replacer.unpin(2);
        replacer.unpin(1);
        replacer.unpin(3);
        replacer.pin(1);
        assert_eq!(replacer.victim(), Some(2));
        assert_eq!(replacer.victim(), Some(3));
        assert_eq!(replacer.victim(), None);
        replacer.unpin(1);
        assert_eq!(replacer.victim(), Some(1));
        assert_eq!(replacer.victim(), None);
        replacer.unpin(4);
        assert_eq!(replacer.victim(), Some(4));
        assert_eq!(replacer.victim(), None);
    }
}
