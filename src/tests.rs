use crate::{HazardPtr, HazardRecord, HazardRegistry, HazardValue};
use std::sync::atomic::Ordering;

pub struct HazardStack<T: Send + Clone> {
    hp: HazardPtr<Vec<T>>,
}

impl<T: Send + Clone> Default for HazardStack<T> {
    fn default() -> Self {
        let registry = HazardRegistry::default();
        let hp = HazardPtr::new(HazardValue::boxed(Vec::new()), &registry);
        HazardStack { hp }
    }
}

impl<T: Send + Clone> HazardStack<T> {
    pub fn push(&self, item: T) {
        let mut record = HazardRecord::default();
        loop {
            let scope = self.hp.protect(&mut record);
            let reference = scope.as_ref().unwrap();
            let mut new = (*reference).clone();

            new.push(item.clone());

            if scope
                .compare_exchange_weak(
                    HazardValue::boxed(new),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        let mut record = HazardRecord::default();
        loop {
            let scope = self.hp.protect(&mut record);
            let reference = scope.as_ref().unwrap();
            let mut new = (*reference).clone();

            let ret = new.pop();

            if scope
                .compare_exchange_weak(
                    HazardValue::boxed(new),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return ret;
            }
        }
    }

    pub fn take_inner(&self) -> Vec<T> {
        let value = self
            .hp
            .swap(HazardValue::boxed(Vec::new()), Ordering::Relaxed);
        value.take().unwrap()
    }
}

#[test]
pub fn test_hazard_vector() {
    use std::sync::Arc;
    use std::thread;

    let thread_count = 128;
    let loop_count = 53;
    let vec1 = Arc::new(HazardStack::<usize>::default());
    let vec2 = Arc::new(HazardStack::<usize>::default());

    let handles: Vec<_> = (0..thread_count)
        .into_iter()
        .map(|idx| {
            let cpy1 = vec1.clone();
            let cpy2 = vec2.clone();
            thread::spawn(move || {
                thread::park();
                let start = idx * loop_count;
                for idx in start..start + loop_count {
                    cpy1.push(idx);
                }
                for _ in 0..loop_count {
                    cpy2.push(cpy1.pop().unwrap());
                }
            })
        })
        .collect();

    for child in handles.iter() {
        child.thread().unpark();
    }

    for child in handles.into_iter() {
        child.join().unwrap();
    }

    let vec1 = vec1.take_inner();
    assert!(vec1.len() == 0, "expected vec1 to be empty");
    let mut vec2 = vec2.take_inner();
    vec2.sort();

    for i in 0..vec2.len() {
        assert!(vec2[i] == i, "unexpected vec2 value");
    }
}
