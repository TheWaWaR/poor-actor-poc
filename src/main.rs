#[macro_use]
extern crate crossbeam_channel as channel;
extern crate parking_lot;

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::collections::HashMap;
use worker::{Worker, RequestTask, EventTask};

fn main() {
    let (simple_handle, simple_controller) = SimpleWorker::default().start();
    let response = simple_controller.send_request(("this is a request/response task".to_string(), true));
    println!("Got response: {:?}", response.recv());
    simple_controller.send(("this is event to send".to_string(), 333));

    simple_handle.join().expect("Join dummy worker failed");
    println!(">>> DONE!");
}

mod worker {
    use super::channel;
    use std::thread;

    pub struct RequestTask<A, R> {
        pub responsor: channel::Sender<R>,
        pub arguments: A,
    }

    #[derive(Debug)]
    pub struct EventTask<A> {
        pub arguments: A,
    }

    pub trait Worker {
        type Controller;
        // Worker's main loop
        fn start(self) -> (thread::JoinHandle<()>, Self::Controller);
    }
}


type Task1ResponseValue = u32;
type Task1Arguments = (String, bool);

type Task2Arguments = (String, u64);


#[derive(Default)]
struct SimpleWorker {
}

#[derive(Clone)]
struct SimpleWorkerController {
    task1_sender: channel::Sender<RequestTask<Task1Arguments, Task1ResponseValue>>,
    task2_sender: channel::Sender<EventTask<Task2Arguments>>,
}

impl Worker for SimpleWorker {
    type Controller = SimpleWorkerController;

    fn start(self) -> (thread::JoinHandle<()>, Self::Controller) {
        let (task1_sender, task1_receiver) = channel::bounded(1024);
        let (task2_sender, task2_receiver) = channel::bounded(1024);

        let thread_builder = thread::Builder::new().name("dummy worker".to_string());
        let join_handle = thread_builder.spawn(move || loop {
            select! {
                recv(task1_receiver, msg) => match msg {
                    Some(RequestTask { responsor, arguments }) => {
                        println!("received {:?} from task1", arguments);
                        let return_value = 42;
                        responsor.send(return_value);
                    },
                    None => println!("Task1 channel is closed"),
                }
                recv(task2_receiver, msg) => match msg {
                    Some(EventTask { arguments }) => {
                        println!("received {:?} from task2. Quit after 2 seconds", arguments);
                        thread::sleep(Duration::from_secs(2));
                        break;
                    },
                    None => println!("Task2 channel is closed"),
                }
            }
        }).expect("Start dummy worker failed");

        (
            join_handle,
            SimpleWorkerController {
                task1_sender,
                task2_sender,
            }
        )
    }
}

impl SimpleWorkerController {
    pub fn send_request(&self, arguments: Task1Arguments) -> channel::Receiver<Task1ResponseValue> {
        let (sender, receiver) = channel::bounded(1);
        self.task1_sender.send(RequestTask {
            responsor: sender,
            arguments,
        });
        receiver
    }

    pub fn send(&self, arguments: Task2Arguments) {
        self.task2_sender.send(EventTask { arguments });
    }
}


/* ================ */
/* ==== PubSub ==== */
/* ================ */

struct PubsubWorker {
}

#[derive(Clone)]
struct PubsubWorkerController {
    new_tx_register: channel::Sender<EventTask<(String, channel::Sender<()>)>>,
    new_tip_register: channel::Sender<EventTask<(String, channel::Sender<Arc<Vec<u8>>>)>>,
    switch_fork_register: channel::Sender<EventTask<(String, channel::Sender<Arc<Vec<u32>>>)>>,
    new_tx_notifier: channel::Sender<()>,
    new_tip_notifier: channel::Sender<Arc<Vec<u8>>>,
    switch_fork_notifier: channel::Sender<Arc<Vec<u32>>>,
}

impl Worker for PubsubWorker {
    type Controller = PubsubWorkerController;

    fn start(self) -> (thread::JoinHandle<()>, Self::Controller) {
        let (register1_sender, register1_receiver) = channel::bounded(2);
        let (register2_sender, register2_receiver) = channel::bounded(2);
        let (register3_sender, register3_receiver) = channel::bounded(2);
        let (event1_sender, event1_receiver) = channel::bounded::<()>(128);
        let (event2_sender, event2_receiver) = channel::bounded(128);
        let (event3_sender, event3_receiver) = channel::bounded(128);

        let thread_builder = thread::Builder::new().name("pubsub worker".to_string());
        let join_handle = thread_builder.spawn(move || {
            let mut new_tx_subscribers: HashMap<String, channel::Sender<()>> = HashMap::new();
            let mut new_tip_subscribers: HashMap<String, channel::Sender<Arc<Vec<u8>>>> = HashMap::new();
            let mut switch_fork_subscribers: HashMap<String, channel::Sender<Arc<Vec<u32>>>> = HashMap::new();
            loop {
                select! {
                    recv(register1_receiver, msg) => match msg {
                        Some(EventTask{ arguments: (name, sender) }) => {
                            println!("Register event1 {:?}", name);
                            new_tx_subscribers.insert(name, sender);
                        },
                        None => println!("Register 1 channel is closed"),
                    }
                    recv(register2_receiver, msg) => match msg {
                        Some(EventTask{ arguments: (name, sender) }) => {
                            println!("Register event2 {:?}", name);
                            new_tip_subscribers.insert(name, sender);
                        },
                        None => println!("Register 2 channel is closed"),
                    }
                    recv(register3_receiver, msg) => match msg {
                        Some(EventTask{ arguments: (name, sender) }) => {
                            println!("Register event3 {:?}", name);
                            switch_fork_subscribers.insert(name, sender);
                        },
                        None => println!("Register 3 channel is closed"),
                    }
                    recv(event1_receiver, msg) => match msg {
                        Some(msg) => {
                            println!("event new tx {:?}", msg);
                            for subscriber in new_tx_subscribers.values() {
                                subscriber.send(msg.clone());
                            }
                        },
                        None => println!("event 1 channel is closed"),
                    }
                }
            }
        }).expect("Start pubsub worker failed");

        (
            join_handle,
            PubsubWorkerController {
                new_tx_register: register1_sender,
                new_tip_register: register2_sender,
                switch_fork_register: register3_sender,
                new_tx_notifier: event1_sender,
                new_tip_notifier: event2_sender,
                switch_fork_notifier: event3_sender,
            }
        )
    }
}

impl PubsubWorkerController {
    pub fn subscribe_net_tx<S: ToString>(&self, name: S) -> channel::Receiver<()> {
        let name = name.to_string();
        let (event1_sender, event1_receiver) = channel::bounded::<()>(128);
        self.new_tx_register.send(EventTask { arguments: (name, event1_sender)});
        event1_receiver
    }
    pub fn subscribe_net_tip<S: ToString>(&self, name: S) -> channel::Receiver<Arc<Vec<u8>>> {
        let name = name.to_string();
        let (event2_sender, event2_receiver) = channel::bounded::<Arc<Vec<u8>>>(128);
        self.new_tip_register.send(EventTask { arguments: (name, event2_sender)});
        event2_receiver
    }
    pub fn subscribe_switch_fork<S: ToString>(&self, name: S) -> channel::Receiver<Arc<Vec<u32>>> {
        let name = name.to_string();
        let (event3_sender, event3_receiver) = channel::bounded::<Arc<Vec<u32>>>(128);
        self.switch_fork_register.send(EventTask { arguments: (name, event3_sender)});
        event3_receiver
    }

    pub fn notify_new_tx(&self) {
        self.new_tx_notifier.send(());
    }
    pub fn notify_new_tip(&self, block: Arc<Vec<u8>>) {
        self.new_tip_notifier.send(block);
    }
    pub fn notify_switch_fork(&self, txs: Arc<Vec<u32>>) {
        self.switch_fork_notifier.send(txs);
    }
}
