use os_thread_local::ThreadLocal;
use std::boxed::Box;
use std::cell::RefCell;
use std::sync::{
    atomic::{AtomicPtr, AtomicUsize, Ordering},
    Arc, Mutex,
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

unsafe impl<T: 'static> Send for WrappedValue<T> {}

impl<T: 'static> HazardTrait for WrappedValue<T> {}

enum HazardValueImpl<'registry, T: 'static> {
    Boxed {
        ptr: *mut WrappedValue<T>,
        registry: Option<&'registry HazardRegistryImpl>,
    },
    Dummy {
        ptr: *mut WrappedValue<T>,
    },
}

impl<'registry, T: 'static> HazardValueImpl<'registry, T> {
    fn is_dummy(ptr: *mut WrappedValue<T>) -> bool {
        let mask = 1_usize;
        ((ptr as usize) & mask) != 0
    }

    fn leak(self) -> *mut WrappedValue<T> {
        let ptr = self.as_ptr();
        std::mem::forget(self);
        ptr
    }

    fn as_ptr(&self) -> *mut WrappedValue<T> {
        match self {
            HazardValueImpl::Boxed { ptr, .. } => *ptr,
            HazardValueImpl::Dummy { ptr } => {
                let ptr = *ptr;
                (ptr as usize >> 1) as *mut WrappedValue<T>
            }
        }
    }
}

impl<'registry, T: 'static> Drop for HazardValueImpl<'registry, T> {
    fn drop(&mut self) {
        if let HazardValueImpl::Boxed { ptr, registry } = self {
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

pub struct HazardValue<'registry, T: 'static>(HazardValueImpl<'registry, T>);

impl<'registry, T: 'static> HazardValue<'registry, T> {
    pub fn boxed(value: T) -> HazardValue<'registry, T> {
        let boxed = Box::new(WrappedValue { value });
        let ptr = Box::into_raw(boxed);
        debug_assert!(
            !HazardValueImpl::is_dummy(ptr),
            "unexpected low bit set in allocation"
        );
        HazardValue(HazardValueImpl::Boxed {
            ptr,
            registry: None,
        })
    }

    pub fn dummy(value: usize) -> HazardValue<'registry, T> {
        let max = usize::MAX >> 1;
        assert!(value <= max, "High bit is needed for internal information");
        HazardValue(HazardValueImpl::Dummy {
            ptr: ((value << 1) | 1) as *mut WrappedValue<T>,
        })
    }

    fn from_ptr(
        ptr: *mut WrappedValue<T>,
        registry: Option<&'registry HazardRegistryImpl>,
    ) -> HazardValue<'registry, T> {
        if HazardValueImpl::is_dummy(ptr) {
            HazardValue(HazardValueImpl::Dummy { ptr })
        } else {
            HazardValue(HazardValueImpl::Boxed { ptr, registry })
        }
    }

    pub fn as_ref(&self) -> Option<&T> {
        if let HazardValueImpl::Boxed { ptr, .. } = self.0 {
            let ptr = unsafe { &*ptr };
            Some(&ptr.value)
        } else {
            None
        }
    }

    pub fn as_mut(&mut self) -> Option<&mut T> {
        if let HazardValueImpl::Boxed { ptr, .. } = self.0 {
            let ptr = unsafe { &mut *ptr };
            Some(&mut ptr.value)
        } else {
            None
        }
    }

    pub fn take(self) -> Option<T> {
        if let HazardValueImpl::Boxed { ptr, .. } = self.0 {
            let boxed = unsafe { Box::from_raw(ptr) };
            self.0.leak();
            Some(boxed.value)
        } else {
            None
        }
    }
}

struct HazardRecordImpl<'registry> {
    record: &'registry AtomicPtr<()>,
    registry: &'registry HazardRegistryImpl,
}

impl<'registry> Drop for HazardRecordImpl<'registry> {
    fn drop(&mut self) {
        HazardRegistryImpl::free(self);
    }
}

impl<'registry> HazardRecordImpl<'registry> {
    fn acquire<T>(&mut self, atomic: &AtomicPtr<WrappedValue<T>>) -> *mut WrappedValue<T> {
        let mut ptr = atomic.load(Ordering::Acquire);
        loop {
            debug_assert!(
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

pub struct HazardRecord<'registry> {
    record: Option<HazardRecordImpl<'registry>>,
}

impl<'registry> Default for HazardRecord<'registry> {
    fn default() -> Self {
        HazardRecord { record: None }
    }
}

pub struct HazardScope<'registry, 'hazard, T: 'static> {
    record: &'hazard mut HazardRecordImpl<'registry>,
    ptr: &'registry HazardPtr<T>,
    current: *mut WrappedValue<T>,
}

impl<'registry, 'hazard, T: 'static> Drop for HazardScope<'registry, 'hazard, T> {
    fn drop(&mut self) {
        self.record.release();
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
            .compare_exchange_weak(self.current, new.0.as_ptr(), success, failure)
        {
            Ok(old) => {
                new.0.leak();
                self.record.release();
                Ok(HazardValue::from_ptr(old, Some(self.record.registry)))
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
            .compare_exchange(self.current, new.0.as_ptr(), success, failure)
        {
            Ok(old) => {
                new.0.leak();
                self.record.release();
                Ok(HazardValue::from_ptr(old, Some(self.record.registry)))
            }
            Err(_) => Err(new),
        }
    }

    pub fn as_ref(&self) -> Option<&T> {
        if HazardValueImpl::is_dummy(self.current) {
            None
        } else {
            let reference = unsafe { &*self.current };
            Some(&reference.value)
        }
    }
}

pub struct HazardPtr<T: 'static> {
    registry: Arc<HazardRegistryImpl>,
    atomic: AtomicPtr<WrappedValue<T>>,
}

impl<T: 'static> HazardPtr<T> {
    pub fn new(value: HazardValue<T>, registry: &HazardRegistry) -> HazardPtr<T> {
        HazardPtr {
            registry: registry.registry.clone(),
            atomic: AtomicPtr::new(value.0.leak()),
        }
    }

    #[must_use]
    pub fn protect<'registry, 'hazard>(
        &'registry self,
        record: &'hazard mut HazardRecord<'registry>,
    ) -> HazardScope<'registry, 'hazard, T> {
        if record.record.is_none() {
            record.record = Some(self.registry.alloc());
        }
        let record = record.record.as_mut().unwrap();
        let current = record.acquire(&self.atomic);
        HazardScope {
            ptr: self,
            record,
            current,
        }
    }

    pub fn swap<'registry>(
        &'registry self,
        new: HazardValue<'registry, T>,
        order: Ordering,
    ) -> HazardValue<'registry, T> {
        let old = self.atomic.swap(new.0.leak(), order);
        HazardValue::from_ptr(old, Some(&self.registry))
    }

    pub fn swap_null(&self, order: Ordering) -> HazardValue<'_, T> {
        let old = self.atomic.swap(std::ptr::null_mut(), order);
        HazardValue::from_ptr(old, Some(&self.registry))
    }

    pub fn store(&self, new: HazardValue<'_, T>, order: Ordering) {
        let old = self.swap(new, order);
        drop(old);
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

impl Drop for HazardSlots {
    fn drop(&mut self) {
        let next = self.next.load(Ordering::Relaxed);
        if next != std::ptr::null_mut() {
            let boxed = unsafe { Box::from_raw(next) };
            drop(boxed);
        }
        self.next = AtomicPtr::default();
    }
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
        registry: &'registry HazardRegistryImpl,
    ) -> HazardRecordImpl<'registry> {
        let lock = self.mutex.lock();
        if self.next.load(Ordering::Acquire) == std::ptr::null_mut() {
            let next = Box::new(HazardSlots::new());
            next.slots[0].0.store(1 as *mut (), Ordering::Release);
            let next = Box::into_raw(next);
            self.next.store(next, Ordering::Release);
            registry.numslots.fetch_add(HP_CHUNKS, Ordering::Relaxed);
            let next = unsafe { &*next };
            return HazardRecordImpl {
                record: &next.slots[0].0,
                registry,
            };
        }
        drop(lock);

        let next = self.next.load(Ordering::Relaxed);
        let next = unsafe { &*next };
        next.alloc(tid, registry)
    }

    fn alloc<'registry>(
        &'registry self,
        tid: usize,
        registry: &'registry HazardRegistryImpl,
    ) -> HazardRecordImpl<'registry> {
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
                return HazardRecordImpl {
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

struct DeletedItem(*mut (), Box<dyn HazardTrait>);

struct DeletedList(Vec<DeletedItem>, *const HazardRegistryImpl);

impl Drop for DeletedList {
    fn drop(&mut self) {
        if self.1 != std::ptr::null() {
            let registry = unsafe { &*self.1 };
            loop {
                if self.0.is_empty() {
                    break;
                }
                let hazards = registry.scan();
                self.0.retain(|DeletedItem(ptr, ..)| {
                    hazards.binary_search(&(*ptr as *mut ())).is_ok()
                });
                std::thread::yield_now();
            }
        }
    }
}

struct HazardRegistryImpl {
    slots: HazardSlots,
    numslots: AtomicUsize,
    deleted: ThreadLocal<RefCell<DeletedList>>,
}

impl HazardRegistryImpl {
    fn alloc(&self) -> HazardRecordImpl {
        let tid = aquire_threadid();
        self.slots.alloc(tid, self)
    }

    fn free(HazardRecordImpl { record, .. }: &mut HazardRecordImpl) {
        debug_assert!(
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
            arr.replace_with(|&mut DeletedList(ref mut old, _)| {
                let boxed = unsafe { Box::from_raw(raw) };
                let boxed = boxed as Box<dyn HazardTrait>;
                old.push(DeletedItem(raw as *mut (), boxed));

                if old.len() >= (5 * self.numslots.load(Ordering::Relaxed)) / 4 {
                    let hazards = self.scan();
                    old.retain(|DeletedItem(ptr, ..)| {
                        hazards.binary_search(&(*ptr as *mut ())).is_ok()
                    });
                }
                DeletedList(old.split_off(0), self as *const Self)
            });
        });
    }
}

pub struct HazardRegistry {
    registry: Arc<HazardRegistryImpl>,
}

impl Default for HazardRegistry {
    fn default() -> Self {
        HazardRegistry {
            registry: Arc::new(HazardRegistryImpl {
                slots: HazardSlots::new(),
                numslots: AtomicUsize::new(HP_CHUNKS),
                deleted: ThreadLocal::new(|| {
                    RefCell::new(DeletedList(Vec::new(), std::ptr::null()))
                }),
            }),
        }
    }
}
