use std::io::{self, Write};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

static LOG_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Copy)]
pub enum Level {
    Info,
    Warn,
    Error,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

pub fn info(msg: &str, fields: &[(&str, String)]) {
    log(Level::Info, msg, fields);
}

pub fn warn(msg: &str, fields: &[(&str, String)]) {
    log(Level::Warn, msg, fields);
}

pub fn error(msg: &str, fields: &[(&str, String)]) {
    log(Level::Error, msg, fields);
}

fn log(level: Level, msg: &str, fields: &[(&str, String)]) {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);

    let mut line = format!(
        "{{\"ts\":{ts},\"level\":\"{}\",\"msg\":\"{}\"",
        level.as_str(),
        escape(msg)
    );
    for (key, value) in fields {
        line.push_str(&format!(",\"{}\":\"{}\"", escape(key), escape(value)));
    }
    line.push('}');

    let _guard = LOG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut out = io::stdout();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

fn escape(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}
