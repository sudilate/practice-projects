#[cfg(target_os = "macos")]
pub mod kqueue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionId(pub usize);

pub trait EventLoop {
    fn run(&mut self) -> std::io::Result<()>;
}
