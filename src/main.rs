use crossbeam::channel::unbounded;
use crossbeam::select;
use std::io::ErrorKind;
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
    // 添加一个新的Channel，Worker使用它来通知“并行”组件已经完成了一个工作单元
    let (pool_result_sender, pool_result_receiver) = unbounded();

    let mut ongoing_work = 0;
    let mut exiting = false;

    // 引入线程池，开两个工作线程
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();

    let _ = thread::spawn(move || loop {
        // 使用 corssbeam 提供的 select! 宏 选择一个就绪工作
        select! {
            recv(work_receiver) -> msg => {
                match msg {
                    Ok(WorkMsg::Work(num)) => {
                        let result_sender = result_sender.clone();
                        let pool_result_sender = pool_result_sender.clone();

                        // 注意，这里正在池上启动一个新的工作单元。
                        ongoing_work += 1;

                        pool.spawn(move || {
                            // 1. 发送结果给「主组件」
                            let _ = result_sender.send(ResultMsg::Result(num));

                            // 2. 让并行组件知道这里完成了一个工作单元
                            let _ = pool_result_sender.send(());
                        });
                    },
                    Ok(WorkMsg::Exit) => {
                        // 注意，这里接收请求并退出
                        exiting = true;

                        // 如果没有正则进行的工作则立即退出
                        if ongoing_work == 0 {
                            let _ = result_sender.send(ResultMsg::Exited);
                            break;
                        }
                    },
                    _ => panic!("Error receiving a WorkMsg."),
                }
            },
            recv(pool_result_receiver) -> _ => {
                if ongoing_work == 0 {
                    panic!("Received an unexpected pool result.");
                }

                // 注意，一个工作单元已经被完成
                ongoing_work -=1;

                // 如果没有正在进行的工作，并且接收到了退出请求，那么就退出
                if ongoing_work == 0 && exiting {
                    let _ = result_sender.send(ResultMsg::Exited);
                    break;
                }
            },
        }
    });

    let _ = work_sender.send(WorkMsg::Work(0));
    let _ = work_sender.send(WorkMsg::Work(1));
    let _ = work_sender.send(WorkMsg::Exit);

    // worker执行计数
    let mut counter = 0;

    loop {
        match result_receiver.recv() {
            Ok(ResultMsg::Result(_)) => {
                // 计数当前完成的工作单元
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
