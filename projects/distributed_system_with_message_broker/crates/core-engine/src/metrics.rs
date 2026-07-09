use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    pub tcp_accepts: AtomicU64,
    pub frames_in: AtomicU64,
    pub append_ok: AtomicU64,
    pub append_err: AtomicU64,
    pub protocol_err: AtomicU64,
    pub active_connections: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_accepts(&self) {
        self.tcp_accepts.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_frames(&self) {
        self.frames_in.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_append_ok(&self) {
        self.append_ok.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_append_err(&self) {
        self.append_err.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_protocol_err(&self) {
        self.protocol_err.fetch_add(1, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self, node_id: &str) -> String {
        format!(
            concat!(
                "# HELP core_engine_tcp_accepts_total Accepted TCP connections\n",
                "# TYPE core_engine_tcp_accepts_total counter\n",
                "core_engine_tcp_accepts_total{{node_id=\"{node}\"}} {accepts}\n",
                "# HELP core_engine_frames_in_total Inbound protocol frames\n",
                "# TYPE core_engine_frames_in_total counter\n",
                "core_engine_frames_in_total{{node_id=\"{node}\"}} {frames}\n",
                "# HELP core_engine_append_ok_total Successful appends\n",
                "# TYPE core_engine_append_ok_total counter\n",
                "core_engine_append_ok_total{{node_id=\"{node}\"}} {ok}\n",
                "# HELP core_engine_append_err_total Failed appends\n",
                "# TYPE core_engine_append_err_total counter\n",
                "core_engine_append_err_total{{node_id=\"{node}\"}} {err}\n",
                "# HELP core_engine_protocol_err_total Protocol decode/handle errors\n",
                "# TYPE core_engine_protocol_err_total counter\n",
                "core_engine_protocol_err_total{{node_id=\"{node}\"}} {proto}\n",
                "# HELP core_engine_active_connections Current TCP connections\n",
                "# TYPE core_engine_active_connections gauge\n",
                "core_engine_active_connections{{node_id=\"{node}\"}} {active}\n"
            ),
            node = escape_label(node_id),
            accepts = self.tcp_accepts.load(Ordering::Relaxed),
            frames = self.frames_in.load(Ordering::Relaxed),
            ok = self.append_ok.load(Ordering::Relaxed),
            err = self.append_err.load(Ordering::Relaxed),
            proto = self.protocol_err.load(Ordering::Relaxed),
            active = self.active_connections.load(Ordering::Relaxed),
        )
    }
}

fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::Metrics;

    #[test]
    fn renders_prometheus_text() {
        let metrics = Metrics::new();
        metrics.inc_accepts();
        metrics.inc_frames();
        metrics.inc_append_ok();
        let text = metrics.render_prometheus("node-1");
        assert!(text.contains("core_engine_tcp_accepts_total{node_id=\"node-1\"} 1"));
        assert!(text.contains("core_engine_frames_in_total{node_id=\"node-1\"} 1"));
        assert!(text.contains("core_engine_append_ok_total{node_id=\"node-1\"} 1"));
    }
}
