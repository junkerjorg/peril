#![feature(int_bits_const)]

use os_thread_local::ThreadLocal;
use std::boxed::Box;
use std::cell::RefCell;
use std::sync::{
    atomic::{AtomicPtr, AtomicUsize, Ordering},
    Mutex,
};

pub mod tests;

static THREADID_COUNTER: AtomicUsize = AtomicUsize::new(1);
thread_local! {
    static THREADID: RefCell<usize> = RefCell::new(0);
}

fn aquire_threadid() -> usize {
    THREADID.with(|threadid| {
        let mut value = *threadid.borrow();
        if value == 0 {
            threadid.replace(THREADID_COUNTER.fetch_add(1, Ordering::Relaxed));
            value = *threadid.borrow();
        }
        value
    })
}

pub trait HazardTrait {}

pub struct WrappedValue<T: 'static> {
    value: T,
}

impl<T: 'static> HazardTrait for WrappedValue<T> {}

pub enum HazardValue<'registry, T: 'static> {
    Boxed {
        ptr: *mut WrappedValue<T>,
        registry: Option<&'registry HazardRegistry>,
    },
    Dummy {
        ptr: *mut WrappedValue<T>,
    },
}

impl<'registry, T: 'static> HazardValue<'registry, T> {
    pub fn boxed(value: T) -> HazardValue<'registry, T> {
        let boxed = Box::new(WrappedValue { value });
        let ptr = Box::into_raw(boxed);
        assert!(
            !Self::is_dummy(ptr),
            "unexpected high bit set in allocation"
        );
        HazardValue::Boxed {
            ptr,
            registry: None,
        }
    }

    pub fn dummy(value: usize) -> HazardValue<'registry, T> {
        let mask = 1_usize << (usize::BITS - 1);
        assert!((value & mask) == 0, "high bit is used to flag dummies");
        HazardValue::Dummy {
            ptr: (value | mask) as *mut WrappedValue<T>,
        }
    }

    fn is_dummy(ptr: *mut WrappedValue<T>) -> bool {
        let mask = 1_usize << (usize::BITS - 1);
        ((ptr as usize) & mask) != 0
    }

    fn from_ptr(
        ptr: *mut WrappedValue<T>,
        registry: Option<&'registry HazardRegistry>,
    ) -> HazardValue<'registry, T> {
        if Self::is_dummy(ptr) {
            HazardValue::Dummy { ptr }
        } else {
            HazardValue::Boxed { ptr, registry }
        }
    }

    fn leak(self) -> *mut WrappedValue<T> {
        let ptr = self.as_ptr();
        std::mem::forget(self);
        ptr
    }

    fn as_ptr(&self) -> *mut WrappedValue<T> {
        match self {
            HazardValue::Boxed { ptr, .. } => *ptr,
            HazardValue::Dummy { ptr } => {
                let ptr = *ptr;
                let mask = 1_usize << (usize::BITS - 1);
                (ptr as usize & !mask) as *mut WrappedValue<T>
            }
        }
    }

    pub fn as_ref(&self) -> Option<&T> {
        if let HazardValue::Boxed { ptr, .. } = self {
            let ptr = unsafe { &**ptr };
            Some(&ptr.value)
        } else {
            None
        }
    }

    pub fn as_mut(&mut self) -> Option<&mut T> {
        if let HazardValue::Boxed { ptr, .. } = self {
            let ptr = unsafe { &mut **ptr };
            Some(&mut ptr.value)
        } else {
            None
        }
    }

    pub fn take(self) -> Option<T> {
        if let HazardValue::Boxed { ptr, .. } = self {
            let boxed = unsafe { Box::from_raw(ptr) };
            self.leak();
            Some(boxed.value)
        } else {
            None
        }
    }
}

impl<'registry, T: 'static> Drop for HazardValue<'registry, T> {
    fn drop(&mut self) {
        if let HazardValue::Boxed { ptr, registry } = self {
            let ptr = *ptr;
            if ptr != std::ptr::null_mut() {
                if let Some(registry) = registry.take() {
                    registry.delete(ptr);
                } else {
                    let boxed = unsafe { Box::from_raw(ptr) };
                    drop(boxed);
                }
            }
        }
    }
}

pub struct HazardPtr<T: 'static> {
    atomic: AtomicPtr<WrappedValue<T>>,
}

pub struct HazardRecord<'registry> {
    record: &'registry AtomicPtr<()>,
    registry: &'registry HazardRegistry,
}

impl<'registry> Drop for HazardRecord<'registry> {
    fn drop(&mut self) {
        HazardRegistry::free(self);
    }
}

impl<'registry> HazardRecord<'registry> {
    fn acquire<T>(&mut self, atomic: &AtomicPtr<WrappedValue<T>>) -> *mut WrappedValue<T> {
        let mut ptr = atomic.load(Ordering::Acquire);
        loop {
            assert!(
                ptr != std::ptr::null_mut(),
                "null is reserved to mark free slots"
            );
            self.record.store(ptr as *mut (), Ordering::Release);
            let after = atomic.load(Ordering::Acquire);
            if ptr == after {
                return ptr;
            }
            ptr = after;
        }
    }

    fn release(&mut self) {
        self.record.store(1 as *mut (), Ordering::Relaxed);
    }
}

pub struct HazardScope<'registry, 'hazard, T: 'static> {
    allocation: &'hazard mut HazardRecord<'registry>,
    ptr: &'registry HazardPtr<T>,
    current: *mut WrappedValue<T>,
}

impl<'registry, 'hazard, T: 'static> Drop for HazardScope<'registry, 'hazard, T> {
    fn drop(&mut self) {
        self.allocation.release();
    }
}

impl<'registry, 'hazard, T: 'static> HazardScope<'registry, 'hazard, T> {
    pub fn compare_exchange_weak(
        self,
        new: HazardValue<'registry, T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<HazardValue<'registry, T>, HazardValue<'registry, T>> {
        match self
            .ptr
            .atomic
            .compare_exchange_weak(self.current, new.as_ptr(), success, failure)
        {
            Ok(old) => {
                new.leak();
                self.allocation.release();
                Ok(HazardValue::from_ptr(old, Some(self.allocation.registry)))
            }
            Err(_) => Err(new),
        }
    }

    pub fn compare_exchange(
        self,
        new: HazardValue<'registry, T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<HazardValue<'registry, T>, HazardValue<'registry, T>> {
        match self
            .ptr
            .atomic
            .compare_exchange(self.current, new.as_ptr(), success, failure)
        {
            Ok(old) => {
                new.leak();
                self.allocation.release();
                Ok(HazardValue::from_ptr(old, Some(self.allocation.registry)))
            }
            Err(_) => Err(new),
        }
    }

    pub fn swap(
        self,
        new: HazardValue<'registry, T>,
        order: Ordering,
    ) -> HazardValue<'registry, T> {
        let old = self.ptr.atomic.swap(new.leak(), order);
        self.allocation.release();
        HazardValue::from_ptr(old, Some(self.allocation.registry))
    }

    pub fn swap_null(self, order: Ordering) -> HazardValue<'registry, T> {
        let old = self.ptr.atomic.swap(std::ptr::null_mut(), order);
        self.allocation.release();
        HazardValue::from_ptr(old, Some(self.allocation.registry))
    }

    pub fn store(self, new: HazardValue<'registry, T>, order: Ordering) {
        let old = self.swap(new, order);
        drop(old);
    }

    pub fn as_ref(&self) -> Option<&T> {
        if HazardValue::is_dummy(self.current) {
            None
        } else {
            let reference = unsafe { &*self.current };
            Some(&reference.value)
        }
    }
}

impl<T: 'static> HazardPtr<T> {
    pub fn new(value: HazardValue<T>) -> HazardPtr<T> {
        HazardPtr {
            atomic: AtomicPtr::new(value.leak()),
        }
    }

    #[must_use]
    pub fn protect<'registry, 'hazard>(
        &'registry self,
        allocation: &'hazard mut HazardRecord<'registry>,
    ) -> HazardScope<'registry, 'hazard, T> {
        let current = allocation.acquire(&self.atomic);
        HazardScope {
            ptr: self,
            allocation,
            current,
        }
    }
}

impl<T: 'static> Drop for HazardPtr<T> {
    fn drop(&mut self) {
        let value = HazardValue::from_ptr(self.atomic.load(Ordering::Relaxed), None);
        drop(value);
    }
}

pub const HP_CHUNKS: usize = 32;

#[repr(align(64))]
struct AtomicPointer(AtomicPtr<()>);

impl Default for AtomicPointer {
    fn default() -> Self {
        AtomicPointer(AtomicPtr::new(std::ptr::null_mut()))
    }
}
struct HazardSlots {
    slots: [AtomicPointer; HP_CHUNKS],
    next: AtomicPtr<HazardSlots>,
    mutex: Mutex<()>,
}

impl HazardSlots {
    fn new() -> HazardSlots {
        HazardSlots {
            slots: Default::default(),
            next: AtomicPtr::default(),
            mutex: Mutex::new(()),
        }
    }

    fn grow<'registry>(
        &'registry self,
        tid: usize,
        registry: &'registry HazardRegistry,
    ) -> HazardRecord<'registry> {
        let lock = self.mutex.lock();
        if self.next.load(Ordering::Acquire) == std::ptr::null_mut() {
            let next = Box::new(HazardSlots::new());
            next.slots[0].0.store(1 as *mut (), Ordering::Release);
            let next = Box::into_raw(next);
            self.next.store(next, Ordering::Release);
            registry.numslots.fetch_add(HP_CHUNKS, Ordering::Relaxed);
            let next = unsafe { &*next };
            return HazardRecord {
                record: &next.slots[0].0,
                registry,
            };
        }
        drop(lock);

        let next = self.next.load(Ordering::Relaxed);
        let next = unsafe { &*next };
        next.alloc(tid, registry)
    }

    pub fn alloc<'registry>(
        &'registry self,
        tid: usize,
        registry: &'registry HazardRegistry,
    ) -> HazardRecord<'registry> {
        for i in 0..HP_CHUNKS {
            let index = (tid + i) % HP_CHUNKS;
            if self.slots[index]
                .0
                .compare_exchange_weak(
                    std::ptr::null_mut(),
                    1 as *mut (),
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return HazardRecord {
                    record: &self.slots[index].0,
                    registry,
                };
            }
        }

        let next = self.next.load(Ordering::Acquire);
        if next != std::ptr::null_mut() {
            let next = unsafe { &*next };
            next.alloc(tid, registry)
        } else {
            self.grow(tid, registry)
        }
    }
}

type DeletedList = Vec<(*mut (), Box<dyn HazardTrait>)>;
pub struct HazardRegistry {
    slots: HazardSlots,
    numslots: AtomicUsize,
    deleted: ThreadLocal<RefCell<DeletedList>>,
}

impl Drop for HazardSlots {
    fn drop(&mut self) {
        let next = self.next.load(Ordering::Relaxed);
        if next != std::ptr::null_mut() {
            let boxed = unsafe { Box::from_raw(next) };
            drop(boxed);
        }
    }
}

impl Default for HazardRegistry {
    fn default() -> Self {
        HazardRegistry {
            slots: HazardSlots::new(),
            numslots: AtomicUsize::new(HP_CHUNKS),
            deleted: ThreadLocal::new(|| RefCell::new(Vec::new())),
        }
    }
}

impl HazardRegistry {
    pub fn alloc(&self) -> HazardRecord {
        let tid = aquire_threadid();
        self.slots.alloc(tid, self)
    }

    fn free(HazardRecord { record, .. }: &mut HazardRecord) {
        assert!(
            record.load(Ordering::Relaxed) != std::ptr::null_mut(),
            "null is reserved to mark free slots"
        );
        record.store(std::ptr::null_mut(), Ordering::Relaxed);
    }

    fn scan(&self) -> Vec<*mut ()> {
        let mut arr = Vec::new();
        let mut slots = &self.slots;
        loop {
            for i in 0..HP_CHUNKS {
                let slot = slots.slots[i].0.load(Ordering::Acquire);
                if slot != std::ptr::null_mut() {
                    arr.push(slot);
                }
            }

            let next = slots.next.load(Ordering::Acquire);
            if next == std::ptr::null_mut() {
                break;
            }

            slots = unsafe { &*next };
        }
        arr.sort();
        arr
    }

    fn delete<T>(&self, raw: *mut WrappedValue<T>) {
        self.deleted.with(|arr| {
            let len;
            {
                let mut arr = arr.borrow_mut();
                let boxed = unsafe { Box::from_raw(raw) };
                arr.push((raw as *mut (), boxed as Box<dyn HazardTrait>));
                len = arr.len();
            }

            if len >= (5 * self.numslots.load(Ordering::Relaxed)) / 4 {
                let old = arr.replace(Vec::new());
                let hazards = self.scan();
                let new_arr: Vec<(*mut (), Box<dyn HazardTrait>)> = old
                    .into_iter()
                    .filter(|(ptr, _)| hazards.binary_search(ptr).is_ok())
                    .collect();
                arr.replace(new_arr);
            }
        });
    }
}
