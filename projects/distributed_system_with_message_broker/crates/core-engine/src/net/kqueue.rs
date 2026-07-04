use crate::net::EventLoop;
use std::io;
use std::os::fd::RawFd;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventFilter {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    pub fd: RawFd,
    pub filter: EventFilter,
}

#[derive(Debug)]
pub struct Kqueue {
    fd: RawFd,
}

impl Kqueue {
    pub fn new() -> io::Result<Self> {
        let fd = unsafe { libc::kqueue() };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { fd })
    }

    pub fn register_read(&self, fd: RawFd) -> io::Result<()> {
        self.register(fd, libc::EVFILT_READ)
    }

    pub fn register_write(&self, fd: RawFd) -> io::Result<()> {
        self.register(fd, libc::EVFILT_WRITE)
    }

    pub fn unregister_write(&self, fd: RawFd) -> io::Result<()> {
        let mut event = make_event(fd, libc::EVFILT_WRITE, libc::EV_DELETE);
        let result = unsafe {
            libc::kevent(
                self.fd,
                &mut event,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ENOENT) {
                return Err(error);
            }
        }

        Ok(())
    }

    pub fn wait(&self, timeout: Option<Duration>) -> io::Result<Vec<Event>> {
        let mut events = [empty_event(); 128];
        let timeout = timeout.map(|duration| libc::timespec {
            tv_sec: duration.as_secs() as libc::time_t,
            tv_nsec: duration.subsec_nanos() as libc::c_long,
        });
        let timeout_ptr = timeout
            .as_ref()
            .map_or(std::ptr::null(), |timeout| timeout as *const libc::timespec);

        let count = unsafe {
            libc::kevent(
                self.fd,
                std::ptr::null(),
                0,
                events.as_mut_ptr(),
                events.len() as i32,
                timeout_ptr,
            )
        };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(events[..count as usize]
            .iter()
            .filter_map(|event| match event.filter {
                libc::EVFILT_READ => Some(Event {
                    fd: event.ident as RawFd,
                    filter: EventFilter::Read,
                }),
                libc::EVFILT_WRITE => Some(Event {
                    fd: event.ident as RawFd,
                    filter: EventFilter::Write,
                }),
                _ => None,
            })
            .collect())
    }

    fn register(&self, fd: RawFd, filter: i16) -> io::Result<()> {
        let mut event = make_event(fd, filter, libc::EV_ADD | libc::EV_ENABLE);
        let result = unsafe {
            libc::kevent(
                self.fd,
                &mut event,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }
}

impl Drop for Kqueue {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

#[derive(Debug, Default)]
pub struct KqueueEventLoop;

impl KqueueEventLoop {
    pub fn new() -> Self {
        Self
    }
}

impl EventLoop for KqueueEventLoop {
    fn run(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn make_event(fd: RawFd, filter: i16, flags: u16) -> libc::kevent {
    libc::kevent {
        ident: fd as libc::uintptr_t,
        filter,
        flags,
        fflags: 0,
        data: 0,
        udata: std::ptr::null_mut(),
    }
}

fn empty_event() -> libc::kevent {
    make_event(0, 0, 0)
}

#[cfg(test)]
mod tests {
    use super::Kqueue;

    #[test]
    fn creates_kqueue() {
        Kqueue::new().expect("kqueue creates");
    }
}
