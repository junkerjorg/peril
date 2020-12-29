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
            let (dummy, reference) = scope.as_ref(Ordering::Relaxed);
            let reference = reference.unwrap();
            let mut new = (*reference).clone();

            new.push(item.clone());

            if scope
                .compare_exchange_weak(
                    dummy,
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
            let (dummy, reference) = scope.as_ref(Ordering::Relaxed);
            let reference = reference.unwrap();
            let mut new = (*reference).clone();

            let ret = new.pop();

            if scope
                .compare_exchange_weak(
                    dummy,
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

    pub fn inner(&self) -> Vec<T> {
        let mut allocation = self.registry.alloc();
        let scope = self.hp.protect(&mut allocation);
        let (_, reference) = scope.as_ref(Ordering::Relaxed);
        reference.unwrap().clone()
    }
}

#[test]
pub fn test_hazard_vector() {
    use std::sync::Arc;
    use std::thread;

    let thread_count = 1024;
    let vec1 = Arc::new(HazardVector::<usize>::default());
    let vec2 = Arc::new(HazardVector::<usize>::default());

    let handles = (0..thread_count).into_iter().map(|idx| {
        let cpy1 = vec1.clone();
        let cpy2 = vec2.clone();
        thread::spawn(move || {
            cpy1.push(idx);
            cpy2.push(cpy1.pop().unwrap());
        })
    });

    for child in handles {
        child.join().unwrap();
    }

    assert!((*vec1).inner().len() == 0, "expected vec1 to be empty");
    let mut vec2 = (*vec2).inner();
    vec2.sort();

    for i in 0..vec2.len() {
        assert!(vec2[i] == i, "unexpected vec2 value");
    }
}
