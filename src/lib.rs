use os_thread_local::ThreadLocal;
use std::boxed::Box;
use std::cell::RefCell;
use std::sync::{
    atomic::{AtomicPtr, AtomicUsize},
    Arc, Mutex,
};

#[cfg(test)]
mod tests;

pub use std::sync::atomic::Ordering;

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

enum HazardValueImpl<'registry, T: Send + ?Sized> {
    Boxed {
        ptr: *mut Box<T>,
        registry: Option<&'registry HazardRegistry<T>>,
    },
    Dummy {
        ptr: *mut Box<T>,
    },
}

impl<'registry, T: Send + ?Sized> HazardValueImpl<'registry, T> {
    fn is_dummy(ptr: *mut Box<T>) -> bool {
        let mask = 1_usize;
        ((ptr as usize) & mask) != 0
    }

    fn leak(self) -> *mut Box<T> {
        let ptr = self.as_ptr();
        std::mem::forget(self);
        ptr
    }

    fn as_ptr(&self) -> *mut Box<T> {
        match self {
            HazardValueImpl::Boxed { ptr, .. } => *ptr,
            HazardValueImpl::Dummy { ptr } => *ptr,
        }
    }
}

impl<'registry, T: Send + ?Sized> Drop for HazardValueImpl<'registry, T> {
    fn drop(&mut self) {
        if let HazardValueImpl::Boxed { ptr, registry } = self {
            let ptr = *ptr;
            if ptr != std::ptr::null_mut() {
                if let Some(registry) = registry.take() {
                    registry.delete(ptr);
                } else {
                    let boxed = unsafe { Arc::from_raw(ptr) };
                    drop(boxed);
                }
            }
        }
    }
}

/// encapsulates a boxed Value or a Dummy (like null)
///
/// # Examples
///
/// ```
/// use peril::HazardValue;
///
/// // creating a boxed string value
/// let boxed = HazardValue::boxed("test");
///
/// // creating a dummy nullptr
/// let dummy = HazardValue::<usize>::dummy(0);
/// ```
pub struct HazardValue<'registry, T: Send + ?Sized>(HazardValueImpl<'registry, T>);

impl<'registry, T: Send> HazardValue<'registry, T> {
    /// create a boxed HazardValue
    ///
    /// # Arguments
    ///
    /// * `value` - A generic value held within the box
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::HazardValue;
    ///
    /// // creating a boxed string value
    /// let boxed = HazardValue::boxed("test");
    /// ```
    pub fn boxed(value: T) -> HazardValue<'registry, T> {
        let boxed = Arc::new(Box::new(value) as Box<T>);
        let ptr = Arc::into_raw(boxed) as *mut Box<T>;
        debug_assert!(
            !HazardValueImpl::is_dummy(ptr),
            "unexpected low bit set in allocation"
        );
        HazardValue(HazardValueImpl::Boxed {
            ptr,
            registry: None,
        })
    }
}

impl<'registry, T: Send + ?Sized> HazardValue<'registry, T> {
    /// create a Dummy HazardValue (like null)
    ///
    /// # Arguments
    ///
    /// * `value` - the dummy pointer value (usually used for null values)
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::HazardValue;
    ///
    /// // creating a dummy nullptr
    /// let dummy = HazardValue::<usize>::dummy(0);
    /// ```
    pub fn dummy(value: usize) -> HazardValue<'registry, T> {
        let max = usize::MAX >> 1;
        assert!(value <= max, "High bit is needed for internal information");
        HazardValue(HazardValueImpl::Dummy {
            ptr: ((value << 1) | 1) as *mut Box<T>,
        })
    }

    fn from_ptr(
        ptr: *mut Box<T>,
        registry: Option<&'registry HazardRegistry<T>>,
    ) -> HazardValue<'registry, T> {
        if HazardValueImpl::is_dummy(ptr) {
            HazardValue(HazardValueImpl::Dummy { ptr })
        } else {
            HazardValue(HazardValueImpl::Boxed {
                ptr: ptr as *mut Box<T>,
                registry,
            })
        }
    }

    /// read the data from a Boxed HazardValue and will return None if the value is a dummy
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::HazardValue;
    ///
    /// // creating a boxed string value
    /// let boxed = HazardValue::boxed("test");
    /// assert!(boxed.as_ref().unwrap() == &"test");
    /// ```
    pub fn as_ref(&self) -> Option<&T> {
        if let HazardValueImpl::Boxed { ptr, .. } = self.0 {
            let ptr = unsafe { &**ptr };
            Some(ptr)
        } else {
            None
        }
    }

    /// check if a value is a Boxed HazardValue
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::HazardValue;
    ///
    /// // creating a boxed string value
    /// let boxed = HazardValue::boxed("test");
    /// assert!(boxed.is_boxed());
    /// ```
    pub fn is_boxed(&self) -> bool {
        match self.0 {
            HazardValueImpl::Boxed { .. } => true,
            HazardValueImpl::Dummy { .. } => false,
        }
    }

    /// check if a value is a Dummy HazardValue
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::HazardValue;
    ///
    /// // creating a dummy value
    /// let dummy = HazardValue::<usize>::dummy(0);
    /// assert!(dummy.is_dummy());
    /// ```
    pub fn is_dummy(&self) -> bool {
        match self.0 {
            HazardValueImpl::Boxed { .. } => false,
            HazardValueImpl::Dummy { .. } => true,
        }
    }

    /// get the (pointer) dummy value of HazardValue or return None if the value is not a dummy
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::HazardValue;
    ///
    /// // creating a dummy value
    /// let dummy = HazardValue::<usize>::dummy(1337);
    /// assert!(dummy.as_dummy().unwrap() == 1337);
    /// ```
    pub fn as_dummy(&self) -> Option<usize> {
        match self.0 {
            HazardValueImpl::Boxed { .. } => None,
            HazardValueImpl::Dummy { ptr } => Some((ptr as usize) >> 1),
        }
    }

    /// clones the data from a Boxed HazardValue if T: Clone or return None if the value is a dummy
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::HazardValue;
    ///
    /// // creating a boxed value
    /// let mut boxed = HazardValue::boxed(1);
    /// let taken = boxed.clone_inner().unwrap();
    /// assert!(taken == 1);
    /// ```
    pub fn clone_inner(self) -> Option<T>
    where
        T: Clone,
    {
        if let HazardValueImpl::Boxed { ptr, .. } = self.0 {
            let boxed = unsafe { Arc::from_raw(ptr) };
            self.0.leak();
            Some((**boxed).clone())
        } else {
            None
        }
    }
}

impl<'registry, T: Send + ?Sized> Clone for HazardValue<'registry, T> {
    /// clones the HazardValue itself
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
    ///
    /// // creating a boxed value
    /// let boxed = HazardValue::boxed(1);
    /// let clone = boxed.clone();
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::new(boxed);
    /// let ok = hp.compare_exchange(&registry, clone, HazardValue::dummy(0), Ordering::Relaxed, Ordering::Relaxed).is_ok();
    /// assert!(ok);
    /// ```
    fn clone(&self) -> Self {
        match self.0 {
            HazardValueImpl::Boxed { ptr, registry } => {
                let boxed = unsafe { Arc::from_raw(ptr) };
                let boxed_clone = boxed.clone();
                Arc::into_raw(boxed);
                HazardValue(HazardValueImpl::Boxed {
                    ptr: Arc::into_raw(boxed_clone) as *mut Box<T>,
                    registry,
                })
            }
            HazardValueImpl::Dummy { ptr } => HazardValue(HazardValueImpl::Dummy { ptr }),
        }
    }
}

struct HazardRecordImpl<'registry> {
    record: &'registry AtomicPtr<()>,
}

impl<'registry> Drop for HazardRecordImpl<'registry> {
    fn drop(&mut self) {
        debug_assert!(
            self.record.load(Ordering::Relaxed) != std::ptr::null_mut(),
            "null is reserved to mark free slots"
        );
        self.record.store(std::ptr::null_mut(), Ordering::Relaxed);
    }
}

impl<'registry> HazardRecordImpl<'registry> {
    fn acquire<T: Send + ?Sized>(&mut self, atomic: &AtomicPtr<Box<T>>) -> *mut Box<T> {
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

/// is used outsite of the CAS loop to cache an expensive operation
///
/// # Examples
///
/// ```
/// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer};
///
/// let registry = HazardRegistry::default();
/// let hp = HazardPointer::new(HazardValue::boxed("test"));
/// let mut record = HazardRecord::default();
/// loop {
///     let scope = hp.protect(&registry, &mut record);
///     //...
///     break;
/// }
/// ```
pub struct HazardRecord<'registry> {
    record: Option<HazardRecordImpl<'registry>>,
}

impl<'registry> Default for HazardRecord<'registry> {
    fn default() -> Self {
        HazardRecord { record: None }
    }
}

/// is the scope where the HazardPointer can be safely read
///
/// # Examples
///
/// ```
/// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
///
/// let registry = HazardRegistry::default();
/// let hp = HazardPointer::new(HazardValue::boxed("test"));
/// let mut record = HazardRecord::default();
/// loop {
///     let scope = hp.protect(&registry, &mut record);
///     // ...
///     if scope.compare_exchange_weak(HazardValue::dummy(0), Ordering::Relaxed, Ordering::Relaxed).is_ok()
///     {
///         break;
///     }
/// }
/// ```
pub struct HazardScope<'registry, 'hazard, T: Send + ?Sized> {
    record: &'hazard mut HazardRecordImpl<'registry>,
    registry: &'registry HazardRegistry<T>,
    ptr: &'hazard HazardPointer<T>,
    current: *mut Box<T>,
}

impl<'registry, 'hazard, T: Send + ?Sized> Drop for HazardScope<'registry, 'hazard, T> {
    fn drop(&mut self) {
        self.record.release();
    }
}

impl<'registry, 'hazard, T: Send + ?Sized> HazardScope<'registry, 'hazard, T> {
    /// compare_exchange_weak version of HazardPointer that needs to use a protected HazardScope as the current value (comperator)
    /// is saved when the potection starts so that the value can be safely read and copied. When the CAS succeeds it will return
    /// the previous value held by the HazardPointer and when the CAS fails it will return the new value we just provided.
    /// for more detail see [compare_exchange_weak](std::sync::atomic::AtomicPtr::compare_exchange_weak)
    ///
    /// # Arguments
    ///
    /// * `new` - the new value the HazardPointer should have when the CAS succeeds (and returned when the CAS fails)
    /// * `success` - the [Ordering](std::sync::atomic::Ordering) when the CAS succeeds
    /// * `failure` - the [Ordering](std::sync::atomic::Ordering) when the CAS fails
    /// * `return` - Result wrapping the old value if the CAS succeeds or the new value if it failed
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::new(HazardValue::boxed("test"));
    /// let mut record = HazardRecord::default();
    /// loop {
    ///     let scope = hp.protect(&registry, &mut record);
    ///     // ...
    ///     if scope.compare_exchange_weak(HazardValue::dummy(0), Ordering::Relaxed, Ordering::Relaxed).is_ok()
    ///     {
    ///         break;
    ///     }
    /// }
    /// ```
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
                Ok(HazardValue::from_ptr(old, Some(self.registry)))
            }
            Err(_) => Err(new),
        }
    }

    /// compare_exchange version of HazardPointer that needs to use a protected HazardScope as the current value (comperator)
    /// is saved when the potection starts so that the value can be safely read and copied. When the CAS succeeds it will return
    /// the previous value held by the HazardPointer and when the CAS fails it will return the new value we just provided.
    /// for more detail see [compare_exchange](std::sync::atomic::AtomicPtr::compare_exchange)
    ///
    /// # Arguments
    ///
    /// * `new` - the new value the HazardPointer should have when the CAS succeeds (and returned when the CAS fails)
    /// * `success` - the [Ordering](std::sync::atomic::Ordering) when the CAS succeeds
    /// * `failure` - the [Ordering](std::sync::atomic::Ordering) when the CAS fails
    /// * `return` - Result wrapping the old value if the CAS succeeds or the new value if it failed
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::new(HazardValue::boxed("test"));
    /// let mut record = HazardRecord::default();
    /// loop {
    ///     let scope = hp.protect(&registry, &mut record);
    ///     // ...
    ///     if scope.compare_exchange(HazardValue::dummy(0), Ordering::Relaxed, Ordering::Relaxed).is_ok()
    ///     {
    ///         break;
    ///     }
    /// }
    /// ```
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
                Ok(HazardValue::from_ptr(old, Some(self.registry)))
            }
            Err(_) => Err(new),
        }
    }

    /// check if the protected value is a dummy
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer};
    ///
    /// let registry = HazardRegistry::<usize>::default();
    /// let hp = HazardPointer::new(HazardValue::dummy(0));
    /// let mut record = HazardRecord::default();
    /// loop {
    ///     let scope = hp.protect(&registry, &mut record);
    ///     assert!(scope.is_dummy());
    ///     // ...
    ///     break;
    /// }
    /// ```
    pub fn is_dummy(&self) -> bool {
        HazardValueImpl::is_dummy(self.current)
    }

    /// read the dummy (pointer) value of a HazardPointer
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer};
    ///
    /// let registry = HazardRegistry::<usize>::default();
    /// let hp = HazardPointer::new(HazardValue::dummy(1337));
    /// let mut record = HazardRecord::default();
    /// loop {
    ///     let scope = hp.protect(&registry, &mut record);
    ///     assert!(scope.as_dummy().unwrap() == 1337);
    ///     // ...
    ///     break;
    /// }
    /// ```
    pub fn as_dummy(&self) -> Option<usize> {
        if HazardValueImpl::is_dummy(self.current) {
            Some((self.current as usize) >> 1)
        } else {
            None
        }
    }

    /// read the boxed value of a HazardPointer and returns None if the value is a dummy
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::new(HazardValue::boxed(0));
    /// let mut record = HazardRecord::default();
    /// loop {
    ///     let scope = hp.protect(&registry, &mut record);
    ///     let new = *(scope.as_ref().unwrap()) + 1;
    ///     match scope.compare_exchange(HazardValue::boxed(new), Ordering::Relaxed, Ordering::Relaxed)
    ///     {
    ///         Ok(old) =>
    ///         {
    ///             assert!(old.as_ref().unwrap() == &0);
    ///             break;
    ///         }
    ///         Err(_) => assert!(false),
    ///     }
    /// }
    /// ```
    pub fn as_ref(&self) -> Option<&T> {
        if HazardValueImpl::is_dummy(self.current) {
            None
        } else {
            let reference = unsafe { &**(self.current as *mut Box<T>) };
            Some(reference)
        }
    }

    /// clone the HazardValue of a protected HazardPointer
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::new(HazardValue::boxed(0));
    /// let mut record = HazardRecord::default();
    /// loop {
    ///     let scope = hp.protect(&registry, &mut record);
    ///     let cloned = scope.clone_value();
    ///     if hp.compare_exchange(&registry, cloned, HazardValue::boxed(1), Ordering::Relaxed, Ordering::Relaxed).is_ok()
    ///     {
    ///        break;
    ///     }
    /// }
    /// ```
    pub fn clone_value(&self) -> HazardValue<'registry, T> {
        if HazardValueImpl::is_dummy(self.current) {
            HazardValue(HazardValueImpl::Dummy { ptr: self.current })
        } else {
            let boxed = unsafe { Arc::from_raw(self.current as *mut Box<T>) };
            let boxed_clone = boxed.clone();
            Arc::into_raw(boxed);
            HazardValue(HazardValueImpl::Boxed {
                ptr: Arc::into_raw(boxed_clone) as *mut Box<T>,
                registry: Some(self.registry),
            })
        }
    }

    /// checks if the underlying atomic value already changed
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::new(HazardValue::boxed(0));
    /// let mut record = HazardRecord::default();
    /// loop {
    ///     let scope = hp.protect(&registry, &mut record);
    ///     assert!(!scope.changed(Ordering::Relaxed));
    ///     if scope.compare_exchange(HazardValue::boxed(1), Ordering::Relaxed, Ordering::Relaxed).is_ok()
    ///     {
    ///         break;
    ///     }
    /// }
    /// ```
    pub fn changed(&self, order: Ordering) -> bool {
        self.current != self.ptr.atomic.load(order)
    }
}

/// is the an atomic pointer that is safe to read when it is protected
///
/// # Examples
///
/// ```
/// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer};
///
/// let registry = HazardRegistry::default();
/// let hp = HazardPointer::new(HazardValue::boxed("test"));
/// let mut record = HazardRecord::default();
/// loop {
///     let scope = hp.protect(&registry, &mut record);
///     // ...
///     break;
/// }
/// ```
pub struct HazardPointer<T: Send + ?Sized> {
    atomic: AtomicPtr<Box<T>>,
}

impl<T: Send + ?Sized> HazardPointer<T> {
    /// create a HazardPointer
    ///
    /// # Arguments
    ///
    /// * `value` - the HazardValue the pointer is initzialized with
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
    ///
    /// let hp = HazardPointer::new(HazardValue::boxed("test"));
    /// ```
    pub fn new(value: HazardValue<T>) -> HazardPointer<T> {
        HazardPointer {
            atomic: AtomicPtr::new(value.0.leak()),
        }
    }

    /// protect the hazardpointer so that it is safe to read and update using a CAS operation
    ///
    /// # Arguments
    ///
    /// * `registry` - the HazardRegistry the pointer is registered with, usually HazardPointers in the same system share a common Registry
    /// * `record` - the HazardRecord, which caches an expensive operation for multiple itterations of the update loop
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer};
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::new(HazardValue::boxed("test"));
    /// let mut record = HazardRecord::default();
    /// loop {
    ///     let scope = hp.protect(&registry, &mut record);
    ///     // ...
    ///     break;
    /// }
    /// ```
    #[must_use]
    pub fn protect<'registry, 'hazard>(
        &'hazard self,
        registry: &'registry HazardRegistry<T>,
        record: &'hazard mut HazardRecord<'registry>,
    ) -> HazardScope<'registry, 'hazard, T> {
        if record.record.is_none() {
            record.record = Some(registry.alloc());
        }
        let record = record.record.as_mut().unwrap();
        let current = record.acquire(&self.atomic);
        HazardScope {
            ptr: self,
            record,
            registry,
            current,
        }
    }

    /// compare_exchange_weak version of HazardPointer that uses a HazardValue as it's current value (comperator)
    /// When the CAS succeeds it will return the previous value held by the HazardPointer and when the CAS fails it will return the new value we just provided.
    /// for more detail see [compare_exchange](std::sync::atomic::AtomicPtr::compare_exchange)
    ///
    /// # Arguments
    ///
    /// * `registry` - the HazardRegistry the pointer is registered with, usually HazardPointers in the same system share a common Registry
    /// * `current` - the HazardValue used as a comparator
    /// * `new` - the new value the HazardPointer should have when the CAS succeeds (and returned when the CAS fails)
    /// * `success` - the [Ordering](std::sync::atomic::Ordering) when the CAS succeeds
    /// * `failure` - the [Ordering](std::sync::atomic::Ordering) when the CAS fails
    /// * `return` - Result wrapping the old value if the CAS succeeds or the new value if it failed
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardPointer, Ordering};
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::<usize>::new(HazardValue::dummy(1));
    /// loop {
    ///     if hp.compare_exchange_weak(&registry, HazardValue::dummy(1), HazardValue::dummy(0), Ordering::Relaxed, Ordering::Relaxed).is_ok()
    ///     {
    ///         break;
    ///     }
    /// }
    /// ```
    pub fn compare_exchange_weak<'registry>(
        &self,
        registry: &'registry HazardRegistry<T>,
        current: HazardValue<'registry, T>,
        new: HazardValue<'registry, T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<HazardValue<'registry, T>, HazardValue<'registry, T>> {
        match self.atomic.compare_exchange_weak(
            current.0.as_ptr(),
            new.0.as_ptr(),
            success,
            failure,
        ) {
            Ok(old) => {
                new.0.leak();
                Ok(HazardValue::from_ptr(old, Some(&registry)))
            }
            Err(_) => Err(new),
        }
    }

    /// compare_exchange version of HazardPointer that uses a HazardValue as it's current value (comperator)
    /// When the CAS succeeds it will return the previous value held by the HazardPointer and when the CAS fails it will return the new value we just provided.
    /// for more detail see [compare_exchange](std::sync::atomic::AtomicPtr::compare_exchange)
    ///
    /// # Arguments
    ///
    /// * `registry` - the HazardRegistry the pointer is registered with, usually HazardPointers in the same system share a common Registry
    /// * `current` - the HazardValue used as a comparator
    /// * `new` - the new value the HazardPointer should have when the CAS succeeds (and returned when the CAS fails)
    /// * `success` - the [Ordering](std::sync::atomic::Ordering) when the CAS succeeds
    /// * `failure` - the [Ordering](std::sync::atomic::Ordering) when the CAS fails
    /// * `return` - Result wrapping the old value if the CAS succeeds or the new value if it failed
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardPointer, Ordering};
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::<usize>::new(HazardValue::dummy(1));
    /// loop {
    ///     if hp.compare_exchange(&registry, HazardValue::dummy(1), HazardValue::dummy(0), Ordering::Relaxed, Ordering::Relaxed).is_ok()
    ///     {
    ///         break;
    ///     }
    /// }
    /// ```
    pub fn compare_exchange<'registry>(
        &self,
        registry: &'registry HazardRegistry<T>,
        current: HazardValue<'registry, T>,
        new: HazardValue<'registry, T>,
        success: Ordering,
        failure: Ordering,
    ) -> Result<HazardValue<'registry, T>, HazardValue<'registry, T>> {
        match self
            .atomic
            .compare_exchange(current.0.as_ptr(), new.0.as_ptr(), success, failure)
        {
            Ok(old) => {
                new.0.leak();
                Ok(HazardValue::from_ptr(old, Some(registry)))
            }
            Err(_) => Err(new),
        }
    }

    /// swaps the value of a HazardPointer returning the old value
    ///
    /// # Arguments
    ///
    /// * `registry` - the HazardRegistry the pointer is registered with, usually HazardPointers in the same system share a common Registry
    /// * `new` - the HazardValue after the swap
    /// * `order` - the [Ordering](std::sync::atomic::Ordering) during the swap operation
    /// * `return` - the HazardValue before the swap
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::new(HazardValue::boxed("test"));
    /// let old = hp.swap(&registry, HazardValue::boxed("test2"), Ordering::Relaxed);
    /// assert!(old.as_ref().unwrap() == &"test");
    /// ```
    pub fn swap<'registry>(
        &self,
        registry: &'registry HazardRegistry<T>,
        new: HazardValue<'registry, T>,
        order: Ordering,
    ) -> HazardValue<'registry, T> {
        let old = self.atomic.swap(new.0.leak(), order);
        HazardValue::from_ptr(old, Some(&registry))
    }

    /// swaps the value of a HazardPointer with a null dummy HazardValue
    ///
    /// # Arguments
    ///
    /// * `registry` - the HazardRegistry the pointer is registered with, usually HazardPointers in the same system share a common Registry
    /// * `order` - the [Ordering](std::sync::atomic::Ordering) during the swap operation
    /// * `return` - the HazardValue before the swap
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::new(HazardValue::boxed("test"));
    /// let old = hp.swap_null(&registry, Ordering::Relaxed);
    /// assert!(old.as_ref().unwrap() == &"test");
    /// let old = hp.swap_null(&registry, Ordering::Relaxed);
    /// assert!(old.as_dummy().unwrap() == 0);
    /// ```
    pub fn swap_null<'registry>(
        &self,
        registry: &'registry HazardRegistry<T>,
        order: Ordering,
    ) -> HazardValue<'registry, T> {
        //swapping 1 here because it represents the dummy 0
        let old = self.atomic.swap(1 as *mut Box<T>, order);
        HazardValue::from_ptr(old, Some(registry))
    }

    /// stores a new value of a HazardPointer and drops the old value, this internally uses a swap operation
    ///
    /// # Arguments
    ///
    /// * `registry` - the HazardRegistry the pointer is registered with, usually HazardPointers in the same system share a common Registry
    /// * `new` - the HazardValue after the store
    /// * `order` - the [Ordering](std::sync::atomic::Ordering) during the store operation
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
    ///
    /// let registry = HazardRegistry::default();
    /// let hp = HazardPointer::new(HazardValue::boxed("test"));
    /// hp.store(&registry, HazardValue::dummy(1337), Ordering::Relaxed);
    /// let old = hp.swap(&registry, HazardValue::boxed("test2"), Ordering::Relaxed);
    /// assert!(old.as_dummy().unwrap() == 1337);
    /// ```
    pub fn store<'registry>(
        &self,
        registry: &'registry HazardRegistry<T>,
        new: HazardValue<'registry, T>,
        order: Ordering,
    ) {
        let old = self.swap(registry, new, order);
        drop(old);
    }

    /// get the value of the HazardPointer but only if it is a dummy
    ///
    /// # Arguments
    ///
    /// * `order` - the [Ordering](std::sync::atomic::Ordering) during the store operation
    ///
    /// # Examples
    ///
    /// ```
    /// use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};
    ///
    /// let hp = HazardPointer::<usize>::new(HazardValue::dummy(1337));
    /// assert!(hp.get_dummy(Ordering::Relaxed).unwrap() == 1337);
    /// ```
    pub fn get_dummy(&self, order: Ordering) -> Option<usize> {
        let ptr = self.atomic.load(order);
        if HazardValueImpl::is_dummy(ptr) {
            Some((ptr as usize) >> 1)
        } else {
            None
        }
    }
}

impl<T: Send + ?Sized> Drop for HazardPointer<T> {
    fn drop(&mut self) {
        let value = HazardValue::from_ptr(self.atomic.load(Ordering::Relaxed), None);
        drop(value);
    }
}

const HP_CHUNKS: usize = 32;

#[repr(align(64))]
struct AtomicPointer(AtomicPtr<()>);

impl Default for AtomicPointer {
    fn default() -> Self {
        AtomicPointer(AtomicPtr::new(std::ptr::null_mut()))
    }
}
struct HazardSlots<T: Send + ?Sized> {
    slots: [AtomicPointer; HP_CHUNKS],
    next: AtomicPtr<HazardSlots<T>>,
    mutex: Mutex<()>,
}

impl<T: Send + ?Sized> Drop for HazardSlots<T> {
    fn drop(&mut self) {
        let next = self.next.load(Ordering::Relaxed);
        if next != std::ptr::null_mut() {
            let boxed = unsafe { Box::from_raw(next) };
            drop(boxed);
        }
        self.next = AtomicPtr::default();
    }
}

impl<T: Send + ?Sized> HazardSlots<T> {
    fn new() -> HazardSlots<T> {
        HazardSlots {
            slots: Default::default(),
            next: AtomicPtr::default(),
            mutex: Mutex::new(()),
        }
    }

    fn grow<'registry>(
        &'registry self,
        tid: usize,
        registry: &'registry HazardRegistry<T>,
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
        registry: &'registry HazardRegistry<T>,
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

struct DeletedItem<T: Send + ?Sized>(*mut (), Arc<Box<T>>);

struct DeletedList<T: Send + ?Sized>(Vec<DeletedItem<T>>, *const HazardRegistry<T>);

impl<T: Send + ?Sized> Drop for DeletedList<T> {
    fn drop(&mut self) {
        if self.1 != std::ptr::null() {
            let registry = unsafe { &*self.1 };
            loop {
                if self.0.is_empty() {
                    break;
                }
                DeletedList::delete(&mut self.0, registry);
                std::thread::yield_now();
            }
        }
    }
}

/// is a registry that stores lifetime information for multiple HazardPointers
///
/// # Examples
///
/// ```
/// use peril::{HazardRegistry, HazardValue, HazardPointer};
///
/// let hp = HazardPointer::new(HazardValue::boxed("test"));
/// ```
pub struct HazardRegistry<T: Send + ?Sized> {
    slots: HazardSlots<T>,
    numslots: AtomicUsize,
    deleted: ThreadLocal<RefCell<DeletedList<T>>>,
}

impl<T: Send + ?Sized> HazardRegistry<T> {
    fn alloc(&self) -> HazardRecordImpl {
        let tid = aquire_threadid();
        self.slots.alloc(tid, self)
    }

    fn scan(&self) -> Vec<*mut ()> {
        let mut arr = Vec::new();
        arr.reserve(self.numslots.load(Ordering::Relaxed));
        let mut slots = &self.slots;
        loop {
            for i in 0..HP_CHUNKS {
                let slot = slots.slots[i].0.load(Ordering::Acquire);
                if slot != std::ptr::null_mut() && !HazardValueImpl::is_dummy(slot as *mut Box<T>) {
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

    fn delete(&self, raw: *mut Box<T>) {
        self.deleted.with(|arr| {
            arr.replace_with(|&mut DeletedList(ref mut old, _)| {
                let boxed = unsafe { Arc::from_raw(raw) };
                old.push(DeletedItem(raw as *mut (), boxed));

                if old.len() >= (5 * self.numslots.load(Ordering::Relaxed)) / 4 {
                    DeletedList::delete(old, self);
                }
                DeletedList(old.split_off(0), self as *const Self)
            });
        });
    }
}

impl<T: Send + ?Sized> DeletedList<T> {
    fn delete(list: &mut Vec<DeletedItem<T>>, registry: &HazardRegistry<T>) {
        let hazards = registry.scan();
        list.retain(|DeletedItem(ptr, ..)| hazards.binary_search(&(*ptr as *mut ())).is_ok());
    }
}

impl<T: Send + ?Sized> Default for HazardRegistry<T> {
    fn default() -> Self {
        HazardRegistry {
            slots: HazardSlots::new(),
            numslots: AtomicUsize::new(HP_CHUNKS),
            deleted: ThreadLocal::new(|| RefCell::new(DeletedList(Vec::new(), std::ptr::null()))),
        }
    }
}
