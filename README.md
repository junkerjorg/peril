# peril
[![Latest version](https://img.shields.io/crates/v/peril.svg)](https://crates.io/crates/peril)
[![Documentation](https://docs.rs/peril/badge.svg)](https://docs.rs/peril)
![Lines of code](https://tokei.rs/b1/github/junkerjorg/peril)
![MIT](https://img.shields.io/badge/license-MIT-blue.svg)

Peril is fast and safe Hazard pointer implementation for Rust. 
Peril uses Chunks of 32 HazardRecords to quickly mark a pointer as protected.
This is less efficent than other implementations that expose thread IDs to the user API, 
but it also frees the user from having to keep track of all active threads in the system. 

## Usage

Add these lines to your `Cargo.toml`:

```toml
[dependencies]
peril = "0.4"
```

than use the HazardPointers in your lock-free update loop:

```rust
use peril::{HazardRegistry, HazardValue, HazardRecord, HazardPointer, Ordering};

let registry = HazardRegistry::default();
let hp = HazardPointer::new(HazardValue::boxed(0), &registry);
let mut record = HazardRecord::default();
loop {
    let scope = hp.protect(&mut record);
    let new = *(scope.as_ref().unwrap()) + 1;
    match scope.compare_exchange(HazardValue::boxed(new), Ordering::Relaxed, Ordering::Relaxed)
    {
        Ok(old) =>
        {
            assert!(old.as_ref().unwrap() == &0);
            break;
        }
        Err(_) => assert!(false),
    }
}
```
 
## License

Licensed under [MIT license](http://opensource.org/licenses/MIT)
