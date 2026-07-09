use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskResult {
    pub output: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub id: String,
    pub payload: Vec<u8>,
    pub status: TaskStatus,
    pub result: Option<TaskResult>,
}

/// Pure in-memory task state machine. Applying a committed entry creates or
/// updates a task. The state machine is intentionally simple: it records that a
/// task with the given id and payload has been accepted by the cluster and is
/// pending execution. Callers can advance statuses and attach results as workers
/// process tasks.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TaskState {
    tasks: HashMap<String, Task>,
}

impl TaskState {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    /// Apply a committed log entry to the state machine. If the task id is
    /// already known the existing status and result are preserved.
    pub fn apply(&mut self, id: String, payload: Vec<u8>) -> &Task {
        self.tasks.entry(id.clone()).or_insert(Task {
            id,
            payload,
            status: TaskStatus::Pending,
            result: None,
        })
    }

    pub fn get(&self, id: &str) -> Option<&Task> {
        self.tasks.get(id)
    }

    pub fn set_status(&mut self, id: &str, status: TaskStatus) -> Option<&Task> {
        let task = self.tasks.get_mut(id)?;
        task.status = status;
        Some(task)
    }

    pub fn set_result(&mut self, id: &str, result: TaskResult) -> Option<&Task> {
        let task = self.tasks.get_mut(id)?;
        task.status = if result.error.is_some() {
            TaskStatus::Failed
        } else {
            TaskStatus::Completed
        };
        task.result = Some(result);
        Some(task)
    }

    pub fn pending_ids(&self) -> Vec<String> {
        self.tasks
            .iter()
            .filter(|(_, task)| task.status == TaskStatus::Pending)
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn ids(&self) -> impl Iterator<Item = &str> {
        self.tasks.keys().map(String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerTask {
    pub id: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResult {
    pub id: String,
    pub output: Option<String>,
    pub error: Option<String>,
}

/// Off-main-thread worker pool for executing committed tasks. The pool owns a
/// single worker thread and communicates via channels so the kqueue event loop
/// never blocks on task execution.
pub struct WorkerPool {
    sender: Sender<WorkerTask>,
    receiver: Receiver<WorkerResult>,
}

impl std::fmt::Debug for WorkerPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerPool").finish()
    }
}

impl WorkerPool {
    pub fn new() -> Self {
        let (task_sender, task_receiver) = channel::<WorkerTask>();
        let (result_sender, result_receiver) = channel::<WorkerResult>();

        thread::spawn(move || {
            while let Ok(task) = task_receiver.recv() {
                let result = execute_task(&task);
                let _ = result_sender.send(WorkerResult {
                    id: task.id,
                    output: result.output,
                    error: result.error,
                });
            }
        });

        Self {
            sender: task_sender,
            receiver: result_receiver,
        }
    }

    pub fn submit(&self, task: WorkerTask) {
        let _ = self.sender.send(task);
    }

    pub fn drain_results(&self) -> Vec<WorkerResult> {
        let mut results = Vec::new();
        while let Ok(result) = self.receiver.try_recv() {
            results.push(result);
        }
        results
    }
}

impl Default for WorkerPool {
    fn default() -> Self {
        Self::new()
    }
}

fn execute_task(task: &WorkerTask) -> TaskResult {
    let payload = match String::from_utf8(task.payload.clone()) {
        Ok(payload) => payload,
        Err(_) => {
            return TaskResult {
                output: None,
                error: Some("invalid utf-8 payload".to_string()),
            }
        }
    };

    let json: serde_json::Value = match serde_json::from_str(&payload) {
        Ok(value) => value,
        Err(error) => {
            return TaskResult {
                output: None,
                error: Some(format!("invalid json payload: {error}")),
            }
        }
    };

    let task_type = json.get("type").and_then(|v| v.as_str()).unwrap_or("echo");
    let input = json
        .get("payload")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let output = match task_type {
        "uppercase" => input.to_uppercase(),
        "reverse" => input.chars().rev().collect(),
        _ => input,
    };

    TaskResult {
        output: Some(output),
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskResult, TaskState, TaskStatus, WorkerPool, WorkerResult, WorkerTask};
    use std::time::{Duration, Instant};

    #[test]
    fn apply_creates_pending_task() {
        let mut state = TaskState::new();

        let task = state.apply("task-1".to_string(), b"payload".to_vec());

        assert_eq!(task.id, "task-1");
        assert_eq!(task.payload, b"payload");
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(state.len(), 1);
    }

    #[test]
    fn apply_is_idempotent() {
        let mut state = TaskState::new();
        state.apply("task-1".to_string(), b"payload".to_vec());
        state.set_status("task-1", TaskStatus::Completed);

        let task = state.apply("task-1".to_string(), b"payload".to_vec());

        assert_eq!(task.status, TaskStatus::Completed);
    }

    #[test]
    fn status_can_be_advanced() {
        let mut state = TaskState::new();
        state.apply("task-1".to_string(), b"payload".to_vec());

        state.set_status("task-1", TaskStatus::Running);
        let task = state.get("task-1").expect("task exists");
        assert_eq!(task.status, TaskStatus::Running);

        state.set_status("task-1", TaskStatus::Completed);
        let task = state.get("task-1").expect("task exists");
        assert_eq!(task.status, TaskStatus::Completed);
    }

    #[test]
    fn result_updates_status_to_completed() {
        let mut state = TaskState::new();
        state.apply("task-1".to_string(), b"payload".to_vec());

        state.set_result(
            "task-1",
            TaskResult {
                output: Some("done".to_string()),
                error: None,
            },
        );

        let task = state.get("task-1").expect("task exists");
        assert_eq!(task.status, TaskStatus::Completed);
        assert_eq!(
            task.result.as_ref().unwrap().output,
            Some("done".to_string())
        );
    }

    #[test]
    fn worker_pool_executes_uppercase_task() {
        let pool = WorkerPool::new();
        let payload = br#"{"id":"t1","type":"uppercase","payload":"hello"}"#.to_vec();
        pool.submit(WorkerTask {
            id: "t1".to_string(),
            payload,
        });

        let results = wait_for_results(&pool, 1, Duration::from_millis(500));

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "t1");
        assert_eq!(results[0].output, Some("HELLO".to_string()));
        assert_eq!(results[0].error, None);
    }

    #[test]
    fn worker_pool_returns_error_for_invalid_json() {
        let pool = WorkerPool::new();
        pool.submit(WorkerTask {
            id: "t1".to_string(),
            payload: b"not json".to_vec(),
        });

        let results = wait_for_results(&pool, 1, Duration::from_millis(500));

        assert_eq!(results.len(), 1);
        assert!(results[0].error.is_some());
    }

    fn wait_for_results(
        pool: &WorkerPool,
        expected: usize,
        timeout: Duration,
    ) -> Vec<WorkerResult> {
        let start = Instant::now();
        let mut results = Vec::new();
        while start.elapsed() < timeout {
            results.extend(pool.drain_results());
            if results.len() >= expected {
                return results;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        results
    }
}
