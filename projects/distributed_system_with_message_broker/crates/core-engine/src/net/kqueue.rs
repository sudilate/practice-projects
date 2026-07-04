use crate::net::EventLoop;

#[derive(Debug, Default)]
pub struct KqueueEventLoop;

impl KqueueEventLoop {
    pub fn new() -> Self {
        Self
    }
}

impl EventLoop for KqueueEventLoop {
    fn run(&mut self) -> std::io::Result<()> {
        // Phase 1.2 will wire raw libc::kqueue/kevent polling here.
        Ok(())
    }
}
