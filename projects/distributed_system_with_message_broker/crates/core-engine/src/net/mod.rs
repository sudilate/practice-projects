#[cfg(target_os = "macos")]
pub mod kqueue;
#[cfg(target_os = "macos")]
pub mod raft;
#[cfg(target_os = "macos")]
pub mod swim;
#[cfg(target_os = "macos")]
pub mod tcp;
#[cfg(target_os = "macos")]
pub mod udp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionId(pub usize);

pub trait EventLoop {
    fn run(&mut self) -> std::io::Result<()>;
}
