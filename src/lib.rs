#![feature(int_bits_const)]

use os_thread_local::ThreadLocal;
use std::boxed::Box;
use std::cell::RefCell;
use std::sync::{
    atomic::{AtomicPtr, AtomicUsize, Ordering},
    Mutex,
};

pub trait HazardTrait {}

macro_rules! array
{
    (@accum (0,  $($_es:expr),*) -> ($($body:tt)*))   => {array!(@as_expr [$($body)*])};
    (@accum (1,  $($es:expr),*)  -> ($($body:tt)*))   => {array!(@accum (0,  $($es),*) -> ($($body)* $($es,)*))};
    (@accum (2,  $($es:expr),*)  -> ($($body:tt)*))   => {array!(@accum (0,  $($es),*) -> ($($body)* $($es,)* $($es,)*))};
    (@accum (3,  $($es:expr),*)  -> ($($body:tt)*))   => {array!(@accum (2,  $($es),*) -> ($($body)* $($es,)*))};
    (@accum (4,  $($es:expr),*)  -> ($($body:tt)*))   => {array!(@accum (2,  $($es,)* $($es),*) -> ($($body)*))};
    (@accum (5,  $($es:expr),*)  -> ($($body:tt)*))   => {array!(@accum (4,  $($es),*) -> ($($body)* $($es,)*))};
    (@accum (6,  $($es:expr),*)  -> ($($body:tt)*))   => {array!(@accum (4,  $($es),*) -> ($($body)* $($es,)* $($es,)*))};
    (@accum (7,  $($es:expr),*)  -> ($($body:tt)*))   => {array!(@accum (4,  $($es),*) -> ($($body)* $($es,)* $($es,)* $($es,)*))};
    (@accum (8,  $($es:expr),*)  -> ($($body:tt)*))   => {array!(@accum (4,  $($es,)* $($es),*) -> ($($body)*))};
    (@accum (16, $($es:expr),*)  -> ($($body:tt)*))   => {array!(@accum (8,  $($es,)* $($es),*) -> ($($body)*))};
    (@accum (32, $($es:expr),*)  -> ($($body:tt)*))   => {array!(@accum (16, $($es,)* $($es),*) -> ($($body)*))};
    (@accum (64, $($es:expr),*)  -> ($($body:tt)*))   => {array!(@accum (32, $($es,)* $($es),*) -> ($($body)*))};

    (@as_expr $e:expr) => {$e};
    [$e:expr; $n:tt] => { array!(@accum ($n, $e) -> ()) };
}

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
        assert!(Self::is_dummy(ptr), "unexpected high bit set in allocation");
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

    fn leak(mut self) -> *mut WrappedValue<T> {
        let ptr = self.as_ptr();
        self = HazardValue::dummy(ptr as usize);
        drop(self);
        ptr
    }

    fn as_ptr(&self) -> *mut WrappedValue<T> {
        match self {
            HazardValue::Boxed { ptr, .. } => *ptr,
            HazardValue::Dummy { ptr } => *ptr,
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
}

impl<'registry, T: 'static> Drop for HazardValue<'registry, T> {
    fn drop(&mut self) {
        if let HazardValue::Boxed { ptr, registry } = self {
            let ptr = *ptr;
            assert!(ptr != std::ptr::null_mut(), "registered values are boxed");

            if let Some(registry) = registry.take() {
                registry.delete(ptr);
            } else {
                let boxed = unsafe { Box::from_raw(ptr) };
                drop(boxed);
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
    fn acquire<T>(&mut self, atomic: &AtomicPtr<WrappedValue<T>>) {
        loop {
            let ptr = atomic.load(Ordering::Acquire);
            assert!(
                ptr != std::ptr::null_mut(),
                "null is reserved to mark free slots"
            );
            self.record.store(ptr as *mut (), Ordering::Release);
            if ptr == atomic.load(Ordering::Acquire) {
                break;
            }
        }
    }

    fn release(&mut self) {
        self.record.store(1 as *mut (), Ordering::Relaxed);
    }
}

pub struct HazardScope<'registry, 'hazard, T: 'static> {
    allocation: &'hazard mut HazardRecord<'registry>,
    ptr: &'registry HazardPtr<T>,
}

impl<'registry, 'hazard, T: 'static> Drop for HazardScope<'registry, 'hazard, T> {
    fn drop(&mut self) {
        self.allocation.release();
    }
}

impl<'registry, 'hazard, T: 'static> HazardScope<'registry, 'hazard, T> {
    pub fn compare_exchange_weak(
        self,
        current: HazardValue<T>,
        new: HazardValue<'registry, T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<HazardValue<'registry, T>, HazardValue<'registry, T>> {
        match self.ptr.atomic.compare_exchange_weak(
            current.as_ptr(),
            new.as_ptr(),
            success,
            failure,
        ) {
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
        current: HazardValue<T>,
        new: HazardValue<'registry, T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<HazardValue<'registry, T>, HazardValue<'registry, T>> {
        match self
            .ptr
            .atomic
            .compare_exchange(current.as_ptr(), new.as_ptr(), success, failure)
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

    pub fn as_ref(&self, order: Ordering) -> (HazardValue<'registry, T>, Option<&T>) {
        let ptr = self.ptr.atomic.load(order);
        if HazardValue::is_dummy(ptr) {
            (HazardValue::dummy(ptr as usize), None)
        } else {
            let reference = unsafe { &*ptr };
            (HazardValue::dummy(ptr as usize), Some(&reference.value))
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
        allocation.acquire(&self.atomic);
        HazardScope {
            ptr: self,
            allocation,
        }
    }
}

impl<T: 'static> Drop for HazardPtr<T> {
    fn drop(&mut self) {
        let value = HazardValue::from_ptr(self.atomic.load(Ordering::Relaxed), None);
        drop(value);
    }
}

struct HazardSlots {
    slots: [AtomicPtr<()>; 64],
    next: AtomicPtr<HazardSlots>,
    mutex: Mutex<()>,
}

impl HazardSlots {
    fn new() -> HazardSlots {
        HazardSlots {
            slots: array![AtomicPtr::default(); 64],
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
        if self.next.load(Ordering::Relaxed) == std::ptr::null_mut() {
            let next = Box::new(HazardSlots::new());
            next.slots[0].store(1 as *mut (), Ordering::Relaxed);
            let next = Box::into_raw(next);
            self.next.store(next, Ordering::Relaxed);
            registry.numslots.fetch_add(64, Ordering::Relaxed);
            let next = unsafe { &*next };
            return HazardRecord {
                record: &next.slots[0],
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
        for i in 0..64 {
            if self.slots[tid + i % 64]
                .compare_exchange_weak(
                    std::ptr::null_mut(),
                    1 as *mut (),
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return HazardRecord {
                    record: &self.slots[tid + i % 64],
                    registry,
                };
            }
        }

        let next = self.next.load(Ordering::Relaxed);
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
        let boxed = unsafe { Box::from_raw(next) };
        drop(boxed);
    }
}

impl Default for HazardRegistry {
    fn default() -> Self {
        HazardRegistry {
            slots: HazardSlots::new(),
            numslots: AtomicUsize::new(64),
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
            for i in 0..64 {
                let slot = slots.slots[i].load(Ordering::Relaxed);
                if slot != std::ptr::null_mut() {
                    arr.push(slot);
                }
            }

            let next = slots.next.load(Ordering::Relaxed);
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
