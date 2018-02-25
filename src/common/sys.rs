use nix::unistd::gethostname;

pub fn get_hostname() -> String {
    let mut buf = [0u8; 256];
    gethostname(&mut buf).unwrap().to_str().unwrap().to_string()
}
