use core_engine::protocol::{Frame, Opcode};

fn main() {
    let frame = Frame::new(Opcode::AppendTask, b"core-engine bootstrap".to_vec());
    println!("core-engine ready: {:?}", frame.opcode());
}
