use crate::{HazardPtr, HazardRegistry, HazardValue};
use std::sync::atomic::Ordering;

pub struct HazardVector<T: 'static> {
    registry: HazardRegistry,
    hp: HazardPtr<Vec<T>>,
}

impl<T: Clone> Default for HazardVector<T> {
    fn default() -> Self {
        HazardVector {
            registry: HazardRegistry::default(),
            hp: HazardPtr::new(HazardValue::boxed(Vec::new())),
        }
    }
}

impl<T: Clone> HazardVector<T> {
    pub fn push(&self, item: T) {
        let mut allocation = self.registry.alloc();
        loop {
            let scope = self.hp.protect(&mut allocation);
            let reference = scope.as_ref();
            let reference = reference.unwrap();
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
        let mut allocation = self.registry.alloc();
        loop {
            let scope = self.hp.protect(&mut allocation);
            let reference = scope.as_ref();
            let reference = reference.unwrap();
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
        let mut allocation = self.registry.alloc();
        let scope = self.hp.protect(&mut allocation);
        let value = scope.swap_null(Ordering::Relaxed);
        value.take().unwrap()
    }
}

#[test]
pub fn test_hazard_vector() {
    use std::sync::Arc;
    use std::thread;

    let thread_count = 128;
    let loop_count = 53;
    let vec1 = Arc::new(HazardVector::<usize>::default());
    let vec2 = Arc::new(HazardVector::<usize>::default());

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
