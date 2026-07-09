use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub id: String,
    pub payload: Vec<u8>,
    pub status: TaskStatus,
}

/// Pure in-memory task state machine. Applying a committed entry creates or
/// updates a task. The state machine is intentionally simple: it records that a
/// task with the given id and payload has been accepted by the cluster and is
/// pending execution. Callers can advance statuses (e.g. `Running`,
/// `Completed`) as workers process tasks.
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
    /// already known the existing status is preserved.
    pub fn apply(&mut self, id: String, payload: Vec<u8>) -> &Task {
        self.tasks.entry(id.clone()).or_insert(Task {
            id,
            payload,
            status: TaskStatus::Pending,
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

#[cfg(test)]
mod tests {
    use super::{TaskState, TaskStatus};

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
}
