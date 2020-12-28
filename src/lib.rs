#![feature(int_bits_const)]

use std::sync::{Mutex, atomic::{AtomicPtr, AtomicUsize, Ordering}};
use std::boxed::Box;
use std::cell::RefCell;
use os_thread_local::ThreadLocal;

pub trait HazardTrait
{
}

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
thread_local! 
{
    static THREADID: RefCell<usize> = RefCell::new(0);
}

fn aquire_threadid() -> usize
{
    THREADID.with(|threadid|
    {
        let mut value = threadid.borrow().clone();
        if value == 0
        {
            threadid.replace(THREADID_COUNTER.fetch_add(1, Ordering::Relaxed));
            value = threadid.borrow().clone();
        }
        value
    })
}

pub struct WrappedValue<T: 'static>
{
    value: T,
}

impl<T: 'static> HazardTrait for WrappedValue<T>
{
}

pub enum HazardValue<'registry, T: 'static>
{
    Boxed
    {
        ptr: *mut WrappedValue<T>, 
        registry: Option<&'registry HazardRegistry>,
    },
    Dummy
    {
        ptr: *mut WrappedValue<T>,
    }
}

impl<'registry, T: 'static> HazardValue<'registry, T>
{
    pub fn boxed(value: T) -> HazardValue<'registry, T>
    {
        let boxed = Box::new(WrappedValue { value });
        let ptr = Box::into_raw(boxed);
        assert!(Self::is_dummy(ptr), "unexpexted high bit set in allocation");
        HazardValue::Boxed { ptr, registry: None }
    }

    pub fn dummy(value: usize) -> HazardValue<'registry, T>
    {
        let mask = (1 as usize) << (usize::BITS - 1);
        assert!((value & mask) == 0, "upper bit is used as flag");
        HazardValue::Dummy { ptr: (value | mask) as *mut WrappedValue<T> }
    }

    fn is_dummy(ptr: *mut WrappedValue<T>) -> bool
    {
        let mask = (1 as usize) << (usize::BITS - 1);
        ((ptr as usize) & mask) != 0
    }

    fn from_ptr(ptr: *mut WrappedValue<T>, registry: Option<&'registry HazardRegistry>) -> HazardValue<'registry, T>
    {
        if Self::is_dummy(ptr)
        {
            HazardValue::Dummy { ptr }
        }
        else
        {
            HazardValue::Boxed { ptr, registry }
        }
    }

    fn leak(mut self) -> *mut WrappedValue<T>
    {
        let ptr = self.as_ptr();
        self = HazardValue::dummy(ptr as usize);
        drop(self);
        ptr
    }

    fn as_ptr(&self) -> *mut WrappedValue<T>
    {
        match self
        {
            HazardValue::Boxed {ptr, ..} => *ptr,
            HazardValue::Dummy {ptr} => *ptr,
        }
    }

    pub fn as_ref(&self) -> Option<&T>
    {
        if let HazardValue::Boxed {ptr, ..} = self
        {
            let ptr = unsafe { & **ptr };
            Some(& ptr.value)
        }
        else
        {
            None
        }
    }

    pub fn as_mut(&mut self) -> Option<&mut T>
    {
        if let HazardValue::Boxed {ptr, ..} = self
        {
            let ptr = unsafe { &mut **ptr };
            Some(&mut ptr.value)
        }
        else
        {
            None
        }
    }
}

impl<'registry, T: 'static> Drop for HazardValue<'registry, T> 
{
    fn drop(&mut self) 
    {
        if let HazardValue::Boxed {ptr, registry} = self
        {
            let ptr = *ptr;
            assert!(ptr != std::ptr::null_mut(), "registered values are boxed");
            let boxed = unsafe { Box::from_raw(ptr) };
            
            if let Some(registry) = registry.take()
            {
                registry.delete(boxed);
            }
        }
    }
}

pub struct HazardPtr<T: 'static>
{
    atomic: AtomicPtr<WrappedValue<T>>,
}

pub struct HazardRecord<'registry>
{
    record: &'registry AtomicPtr<()>,
    registry: &'registry HazardRegistry,
}

impl<'registry> Drop for HazardRecord<'registry>
{
    fn drop(&mut self)
    {
        HazardRegistry::free(self);
    }
}

impl<'registry> HazardRecord<'registry>
{
    fn acquire<T>(&mut self, atomic: &AtomicPtr<WrappedValue<T>>)
    {
        loop 
        {
            let ptr = atomic.load(Ordering::Acquire);
            assert!(ptr != std::ptr::null_mut(), "null is reserved to mark free slots");
            self.record.store(ptr as *mut (), Ordering::Release);
            if ptr == atomic.load(Ordering::Acquire)
            {
                break;
            }
        }
    }

    fn release(&mut self)
    {
        self.record.store(1 as *mut (), Ordering::Relaxed);
    }
}

pub struct HazardScope<'registry, 'hazard, T: 'static> 
{
    allocation: &'hazard mut HazardRecord<'registry>,
    ptr: &'registry HazardPtr<T>,
}

impl<'registry, 'hazard, T: 'static> Drop for HazardScope<'registry, 'hazard, T>
{
    fn drop(&mut self)
    {
        self.allocation.release();
    }
}

impl<'registry, 'hazard, T: 'static> HazardScope<'registry, 'hazard, T>
{
    pub fn compare_exchange_weak(self, current: HazardValue<T>, new: HazardValue<'registry, T>, success: Ordering, failure: Ordering) -> Result<HazardValue<'registry, T>, HazardValue<'registry, T>>
    {
        match self.ptr.atomic.compare_exchange_weak(current.as_ptr(), new.as_ptr(), success, failure)
        {
            Ok(old) =>
            {
                new.leak();
                self.allocation.release();
                Ok(HazardValue::from_ptr(old, Some(self.allocation.registry)))
            }
            Err(_) =>
            {
                Err(new)
            }
        }
    }

    pub fn compare_exchange(self, current: HazardValue<T>, new: HazardValue<'registry, T>, success: Ordering, failure: Ordering) -> Result<HazardValue<'registry, T>, HazardValue<'registry, T>>
    {
        match self.ptr.atomic.compare_exchange(current.as_ptr(), new.as_ptr(), success, failure)
        {
            Ok(old) =>
            {
                new.leak();
                self.allocation.release();
                Ok(HazardValue::from_ptr(old, Some(self.allocation.registry)))
            }
            Err(_) =>
            {
                Err(new)
            }
        }
    }

    pub fn swap(self, new: HazardValue<'registry, T>, order: Ordering) -> HazardValue<'registry, T>
    {
        let old = self.ptr.atomic.swap(new.leak(), order);
        self.allocation.release();
        HazardValue::from_ptr(old, Some(self.allocation.registry))
    }

    pub fn swap_null(self, order: Ordering) -> HazardValue<'registry, T>
    {
        let old =  self.ptr.atomic.swap(std::ptr::null_mut(), order);
        self.allocation.release();
        HazardValue::from_ptr(old, Some(self.allocation.registry))
    }

    pub fn store(self, new: HazardValue<'registry, T>, order: Ordering)
    {
        let old = self.swap(new, order);
        drop(old);
    }

    pub fn as_ref(&self, order: Ordering) -> (HazardValue<'registry, T>, Option<&T>)
    {
        let ptr = self.ptr.atomic.load(order);
        if HazardValue::is_dummy(ptr)
        {
            (HazardValue::dummy(ptr as usize), None)
        }
        else
        {
            let reference = unsafe { & *ptr };
            (HazardValue::dummy(ptr as usize), Some(&reference.value))
        }
    }
}

impl<T: 'static> HazardPtr<T>
{
    pub fn new<'registry>(value: HazardValue<'registry, T>) -> HazardPtr<T>
    {
        HazardPtr { atomic: AtomicPtr::new(value.leak()) }
    }

    #[must_use]
    pub fn protect<'registry, 'hazard>(&'registry self, allocation: &'hazard mut HazardRecord<'registry>) -> HazardScope<'registry, 'hazard, T>
    {
        allocation.acquire(&self.atomic);
        HazardScope { ptr: self, allocation }
    }
}

impl<T: 'static> Drop for HazardPtr<T>
{
    fn drop(&mut self)
    {
        let value = HazardValue::from_ptr(self.atomic.load(Ordering::Relaxed), None);
        drop(value);
    }
}

pub struct HazardRegistry
{
    slots : [AtomicPtr<()>; 64],
    next: AtomicPtr<HazardRegistry>,
    mutex: Mutex<()>,
    deleted: ThreadLocal<RefCell<Vec<Box<dyn HazardTrait>>>>,
}

impl Drop for HazardRegistry
{
    fn drop(&mut self)
    {
        let next = self.next.load(Ordering::Relaxed);
        let boxed = unsafe { Box::from_raw(next) };
        drop(boxed);
    }
}

impl HazardRegistry
{
    pub fn new() -> HazardRegistry
    {
        HazardRegistry{ slots: array![AtomicPtr::default(); 64], next: AtomicPtr::default(), mutex: Mutex::new(()), deleted: ThreadLocal::new(|| RefCell::new(Vec::new())) }
    }

    fn grow(&self) -> HazardRecord
    {
        {
            let _ = self.mutex.lock();
            if self.next.load(Ordering::Relaxed) == std::ptr::null_mut()
            {
                let next = Box::new(HazardRegistry::new());
                next.slots[0].store(1 as *mut (), Ordering::Relaxed);
                let next = Box::into_raw(next);
                self.next.store(next, Ordering::Relaxed);
                let next = unsafe { & *next };
                return HazardRecord { record: &next.slots[0], registry: self };
            }
        }
        let next = self.next.load(Ordering::Relaxed);
        let next = unsafe { & *next };
        next.alloc()
    }

    pub fn alloc(&self) -> HazardRecord
    {
        let tid = aquire_threadid();
        for i in 0..64 
        {
            if let Ok(_) = self.slots[tid + i % 64].compare_exchange_weak(std::ptr::null_mut(), 1 as *mut (), Ordering::Release, Ordering::Relaxed)
            {
                return HazardRecord { record: &self.slots[tid + i % 64], registry: self };
            }
        }

        let next = self.next.load(Ordering::Relaxed);
        if next != std::ptr::null_mut()
        {
            let next = unsafe { & *next };
            next.alloc()
        }
        else
        {
            self.grow()
        }
    }

    fn free(HazardRecord{record, ..}: &mut HazardRecord)
    {
        assert!(record.load(Ordering::Relaxed) != std::ptr::null_mut(), "null is reserved to mark free slots");
        record.store(std::ptr::null_mut(), Ordering::Relaxed);
    }

    fn delete<T: HazardTrait + 'static>(&self, value: Box<T>)
    {
        self.deleted.with(|arr| arr.borrow_mut().push(value as Box<dyn HazardTrait>));
    }
}
