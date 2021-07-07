use crossbeam::channel::unbounded;
use std::thread;

// 此消息用于发送到与「主组件」并行运行的其他组件。
enum WorkMsg {
    Work(u8),
    Exit,
}

// 此消息用于从并行运行的其他组件 发送回「主组件」。
enum ResultMsg {
    Result(u8),
    Exited,
}

fn main() {
    let (work_sender, work_receiver) = unbounded();
    let (result_sender, result_receiver) = unbounded();

    // 生成子线程用于执行另一个并行组件
    let _ = thread::spawn(move || loop {
        // 接收并处理消息，直到收到 exit 消息
        match work_receiver.recv() {
            Ok(WorkMsg::Work(num)) => {
                // 执行一些工作，并且发送消息给 Result 队列
                let _ = result_sender.send(ResultMsg::Result(num));
            }
            Ok(WorkMsg::Exit) => {
                // 发送 exit 确认消息
                let _ = result_sender.send(ResultMsg::Exited);
                break;
            }
            _ => panic!("Error receiving a WorkMsg."),
        }
    });

    let _ = work_sender.send(WorkMsg::Work(0));
    let _ = work_sender.send(WorkMsg::Work(1));
    let _ = work_sender.send(WorkMsg::Exit);

    // worker执行计数
    let mut counter = 0;

    loop {
        match result_receiver.recv() {
            Ok(ResultMsg::Result(num)) => {
                // 断言确保接收和发送的顺序是一致的
                assert_eq!(num, counter);
                counter += 1;
            }
            Ok(ResultMsg::Exited) => {
                // 断言确保在接收两条工作消息之后收到退出消息
                assert_eq!(2, counter);
                break;
            }
            _ => panic!("Error receiving a ResultMsg."),
        }
    }
}
