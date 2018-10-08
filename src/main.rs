#[macro_use]
extern crate crossbeam_channel as channel;
extern crate parking_lot;

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use worker::{RequestTask, Worker};

fn main() {
    let shared = Arc::new(RwLock::new(HashMap::new()));
    let (pubsub_handle, pubshb_controller) = PubsubWorker::default().start("pubsub worker");

    let (simple_handle, simple_controller) =
        SimpleWorker::new(pubshb_controller.clone(), "simple", Arc::clone(&shared)).start("simple");
    let (simple2_handle, simple2_controller) =
        SimpleWorker::new(pubshb_controller.clone(), "simple2", shared).start("simple2");

    let response =
        simple_controller.send_request(("this is a request/response task".to_string(), true));
    println!("Got response: {:?}", response.recv());

    thread::sleep_ms(100);

    simple_controller.do_task2("this is event 1 to send".to_string(), 111);
    simple2_controller.do_task2("this is event 2 to send".to_string(), 222);

    simple_handle.join().expect("join simple worker failed");
    simple2_handle.join().expect("join simple2 worker failed");
    pubshb_controller.stop();
    pubsub_handle.join().expect("join pubsub worker failed");
    println!(">>> DONE!");
}

mod worker {
    use super::channel;
    use std::thread::JoinHandle;

    pub struct RequestTask<A, R> {
        pub responsor: channel::Sender<R>,
        pub arguments: A,
    }

    pub trait Worker {
        type Controller;
        // Worker's main loop
        fn start<S: ToString>(self, thread_name: S) -> (JoinHandle<()>, Self::Controller);
    }
}

/* ================ */
/* ==== Simple ==== */
/* ================ */
type Task1ResponseValue = u32;
type Task1Arguments = (String, bool);

struct SimpleWorker {
    pubsub: PubsubController,
    new_tx_receiver: channel::Receiver<()>,
    shared: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

#[derive(Clone)]
struct SimpleController {
    task1_sender: channel::Sender<RequestTask<Task1Arguments, Task1ResponseValue>>,
    task2_sender: channel::Sender<(String, u64)>,
}

impl Worker for SimpleWorker {
    type Controller = SimpleController;

    fn start<S: ToString>(self, thread_name: S) -> (JoinHandle<()>, Self::Controller) {
        let (task1_sender, task1_receiver) = channel::bounded(1024);
        let (task2_sender, task2_receiver) = channel::bounded(1024);

        let thread_builder = thread::Builder::new().name(thread_name.to_string());
        let join_handle = thread_builder
            .spawn(move || loop {
                select! {
                    recv(task1_receiver, msg) => match msg {
                        Some(request) => {
                            self.handle_request(request);
                        },
                        None => println!("Task1 channel is closed"),
                    }
                    recv(task2_receiver, msg) => match msg {
                        Some((name, value)) => {
                            if self.handle_task2(name, value) {
                                println!("shared: {:?}", *self.shared.read());
                                break;
                            }
                        },
                        None => println!("Task2 channel is closed"),
                    }
                    recv(self.new_tx_receiver, msg) => match msg {
                        Some(_) => {
                            self.handle_new_tx();
                        },
                        None => println!("new tx notify channel is closed"),
                    }
                }
            }).expect("Start dummy worker failed");

        (
            join_handle,
            SimpleController {
                task1_sender,
                task2_sender,
            },
        )
    }
}

impl SimpleWorker {
    pub fn new(
        pubsub: PubsubController,
        new_tx_name: &str,
        shared: Arc<RwLock<HashMap<String, Vec<String>>>>,
    ) -> SimpleWorker {
        let new_tx_receiver = pubsub.subscribe_net_tx(new_tx_name.to_string());
        SimpleWorker {
            new_tx_receiver,
            pubsub,
            shared,
        }
    }

    pub fn handle_request(&self, request: RequestTask<Task1Arguments, Task1ResponseValue>) {
        let RequestTask {
            responsor,
            arguments,
        } = request;
        println!("received {:?} from task1", arguments);
        let return_value = 42;
        responsor.send(return_value);
        self.pubsub.notify_new_tx();
    }

    pub fn handle_task2(&self, name: String, value: u64) -> bool {
        println!(
            "received {:?} from task2. Quit after 1 seconds",
            (&name, value)
        );
        self.shared.write().insert(name, vec![value.to_string()]);
        thread::sleep(Duration::from_secs(1));
        true
    }

    pub fn handle_new_tx(&self) {
        println!("new tx received in SimpleWorker");
    }
}

impl SimpleController {
    pub fn send_request(&self, arguments: Task1Arguments) -> channel::Receiver<Task1ResponseValue> {
        let (sender, receiver) = channel::bounded(1);
        self.task1_sender.send(RequestTask {
            responsor: sender,
            arguments,
        });
        receiver
    }

    pub fn do_task2(&self, name: String, value: u64) {
        self.task2_sender.send((name, value));
    }
}

/* ================ */
/* ==== PubSub ==== */
/* ================ */
type SubscribeAck = ();
type MsgSignal = ();
type MsgNewTx = ();
type MsgNewTip = Arc<Vec<u8>>;
type MsgSwitchFork = Arc<Vec<u32>>;
type PubsubRegister<M> = channel::Sender<RequestTask<(String, channel::Sender<M>), SubscribeAck>>;

#[derive(Default)]
struct PubsubWorker {
    new_tx_subscribers: HashMap<String, channel::Sender<MsgNewTx>>,
    new_tip_subscribers: HashMap<String, channel::Sender<MsgNewTip>>,
    switch_fork_subscribers: HashMap<String, channel::Sender<MsgSwitchFork>>,
}

#[derive(Clone)]
struct PubsubController {
    signal: channel::Sender<MsgSignal>,
    new_tx_register: PubsubRegister<MsgNewTx>,
    new_tip_register: PubsubRegister<MsgNewTip>,
    switch_fork_register: PubsubRegister<MsgSwitchFork>,
    new_tx_notifier: channel::Sender<MsgNewTx>,
    new_tip_notifier: channel::Sender<MsgNewTip>,
    switch_fork_notifier: channel::Sender<MsgSwitchFork>,
}

impl Worker for PubsubWorker {
    type Controller = PubsubController;

    fn start<S: ToString>(mut self, thread_name: S) -> (JoinHandle<()>, Self::Controller) {
        let (signal_sender, signal_receiver) = channel::bounded::<()>(1);
        let (register1_sender, register1_receiver) = channel::bounded(2);
        let (register2_sender, register2_receiver) = channel::bounded(2);
        let (register3_sender, register3_receiver) = channel::bounded(2);
        let (event1_sender, event1_receiver) = channel::bounded::<MsgNewTx>(128);
        let (event2_sender, event2_receiver) = channel::bounded::<MsgNewTip>(128);
        let (event3_sender, event3_receiver) = channel::bounded::<MsgSwitchFork>(128);

        let thread_builder = thread::Builder::new().name(thread_name.to_string());
        let join_handle = thread_builder
            .spawn(move || loop {
                select! {
                    recv(signal_receiver, msg) => {
                        break;
                    }

                    recv(register1_receiver, msg) => match msg {
                        Some(RequestTask { responsor, arguments: (name, sender) }) => {
                            println!("Register event1 {:?}", name);
                            self.new_tx_subscribers.insert(name, sender);
                            responsor.send(());
                        },
                        None => println!("Register 1 channel is closed"),
                    }
                    recv(register2_receiver, msg) => match msg {
                        Some(RequestTask { responsor, arguments: (name, sender)}) => {
                            println!("Register event2 {:?}", name);
                            self.new_tip_subscribers.insert(name, sender);
                            responsor.send(());
                        },
                        None => println!("Register 2 channel is closed"),
                    }
                    recv(register3_receiver, msg) => match msg {
                        Some(RequestTask { responsor, arguments: (name, sender)}) => {
                            println!("Register event3 {:?}", name);
                            self.switch_fork_subscribers.insert(name, sender);
                            responsor.send(());
                        },
                        None => println!("Register 3 channel is closed"),
                    }

                    recv(event1_receiver, msg) => match msg {
                        Some(msg) => {
                            println!("event new tx {:?}", msg);
                            for (name, subscriber) in &self.new_tx_subscribers {
                                println!("Send new tx to: {}", name);
                                subscriber.send(msg.clone());
                            }
                        },
                        None => println!("event 1 channel is closed"),
                    }
                    recv(event2_receiver, msg) => match msg {
                        Some(msg) => {
                            println!("event new tip {:?}", msg);
                            for subscriber in self.new_tip_subscribers.values() {
                                subscriber.send(msg.clone());
                            }
                        },
                        None => println!("event 1 channel is closed"),
                    }
                    recv(event3_receiver, msg) => match msg {
                        Some(msg) => {
                            println!("event switch fork {:?}", msg);
                            for subscriber in self.switch_fork_subscribers.values() {
                                subscriber.send(msg.clone());
                            }
                        },
                        None => println!("event 3 channel is closed"),
                    }
                }
            }).expect("Start pubsub worker failed");

        (
            join_handle,
            PubsubController {
                new_tx_register: register1_sender,
                new_tip_register: register2_sender,
                switch_fork_register: register3_sender,
                new_tx_notifier: event1_sender,
                new_tip_notifier: event2_sender,
                switch_fork_notifier: event3_sender,
                signal: signal_sender,
            },
        )
    }
}

impl PubsubController {
    pub fn stop(&self) {
        self.signal.send(());
    }

    pub fn subscribe_net_tx<S: ToString>(&self, name: S) -> channel::Receiver<MsgNewTx> {
        let name = name.to_string();
        let (responsor, response) = channel::bounded(1);
        let (event1_sender, event1_receiver) = channel::bounded::<MsgNewTx>(128);
        self.new_tx_register.send(RequestTask {
            responsor,
            arguments: (name, event1_sender),
        });
        // Ensure the subscriber is registered.
        let _ = response.recv().expect("Subscribe new tx failed");
        event1_receiver
    }
    pub fn subscribe_net_tip<S: ToString>(&self, name: S) -> channel::Receiver<MsgNewTip> {
        let name = name.to_string();
        let (responsor, response) = channel::bounded(1);
        let (event2_sender, event2_receiver) = channel::bounded::<MsgNewTip>(128);
        self.new_tip_register.send(RequestTask {
            responsor,
            arguments: (name, event2_sender),
        });
        // Ensure the subscriber is registered.
        let _ = response.recv().expect("Subscribe new tip failed");
        event2_receiver
    }
    pub fn subscribe_switch_fork<S: ToString>(&self, name: S) -> channel::Receiver<MsgSwitchFork> {
        let name = name.to_string();
        let (responsor, response) = channel::bounded(1);
        let (event3_sender, event3_receiver) = channel::bounded::<MsgSwitchFork>(128);
        self.switch_fork_register.send(RequestTask {
            responsor,
            arguments: (name, event3_sender),
        });
        // Ensure the subscriber is registered.
        let _ = response.recv().expect("Subscribe switch fork failed");
        event3_receiver
    }

    pub fn notify_new_tx(&self) {
        self.new_tx_notifier.send(());
    }
    pub fn notify_new_tip(&self, block: MsgNewTip) {
        self.new_tip_notifier.send(block);
    }
    pub fn notify_switch_fork(&self, txs: MsgSwitchFork) {
        self.switch_fork_notifier.send(txs);
    }
}
