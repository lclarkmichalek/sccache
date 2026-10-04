use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::FromRawFd;
use std::os::unix::process::CommandExt;
use std::process::Command;

unsafe extern "C" {
    fn ioctl(fd: i32, request: usize, ...) -> i32;
    fn kill(pid: i32, signal: i32) -> i32;
}

fn main() {
    // Prevent rustup detection from bypassing this wrapper.
    if std::env::args().any(|arg| arg == "+stable") {
        std::process::exit(1);
    }
    if let Ok(flags) = std::env::var("CARGO_MAKEFLAGS") {
        let fds = flags
            .split_whitespace()
            .find_map(|flag| flag.strip_prefix("--jobserver-auth="))
            .unwrap();
        let (read, write) = fds.split_once(',').unwrap();
        let read: i32 = read.parse().unwrap();
        let write: i32 = write.parse().unwrap();
        let mut available: i32 = 0;
        assert_eq!(unsafe { ioctl(read, 0x541B, &mut available) }, 0);
        println!("jobserver tokens: {available}");
        std::io::stdout().flush().unwrap();
        assert!(available > 0);
        let mut reader = unsafe { File::from_raw_fd(read) };
        let mut token = [0];
        reader.read_exact(&mut token).unwrap();
        if let Some(ready) = std::env::var_os("SCCACHE_TEST_PAUSE_COMPILER") {
            let ready = std::path::PathBuf::from(ready);
            let pending = ready.with_extension("tmp");
            std::fs::write(&pending, format!("{available} {}", std::process::id())).unwrap();
            std::fs::rename(pending, ready).unwrap();
            std::thread::sleep(std::time::Duration::from_secs(30));
            std::process::exit(1);
        }
        if std::env::var_os("SCCACHE_TEST_KILL_COMPILER").is_some() {
            unsafe { kill(std::process::id() as i32, 9) };
            unreachable!();
        }
        let mut writer = unsafe { File::from_raw_fd(write) };
        writer.write_all(&token).unwrap();
        // The real compiler still needs the inherited descriptors after exec.
        std::mem::forget(reader);
        std::mem::forget(writer);
    }
    let error = Command::new(std::env::var_os("SCCACHE_TEST_REAL_RUSTC").unwrap())
        .args(std::env::args_os().skip(1))
        .exec();
    panic!("failed to exec rustc: {error}");
}
