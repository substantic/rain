#![allow(non_upper_case_globals)]
bitflags! {
    pub struct KeepPolicy: u32 {
        const Client       = 0b00000001;
        const BorderObject = 0b00000010;
        const Snapshot     = 0b00000100;
    }
}
