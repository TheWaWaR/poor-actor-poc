#[macro_use]
extern crate crossbeam_channel as channel;
extern crate parking_lot;
extern crate fnv;

use channel::{Receiver, Sender};
use parking_lot::RwLock;
use service::{Request, Service};
use fnv::FnvHashMap;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

fn main() {
    let shared = Arc::new(RwLock::new(FnvHashMap::default()));
    let (pubsub_handle, pubshb_controller) = PubsubService::default().start("pubsub service");

    let (simple_handle, simple_controller) =
        SimpleService::new(pubshb_controller.clone(), "simple", Arc::clone(&shared))
            .start("simple");
    let (simple2_handle, simple2_controller) =
        SimpleService::new(pubshb_controller.clone(), "simple2", shared).start("simple2");

    let response =
        simple_controller.send_request(("this is a request/response task".to_string(), true));
    println!("Got response: {:?}", response.recv());

    thread::sleep_ms(100);

    simple_controller.do_task2("this is event 1 to send".to_string(), 111);
    simple2_controller.do_task2("this is event 2 to send".to_string(), 222);

    simple_handle.join().expect("join simple service failed");
    simple2_handle.join().expect("join simple2 service failed");
    pubshb_controller.stop();
    pubsub_handle.join().expect("join pubsub service failed");
    println!(">>> DONE!");
}

mod service {
    use super::channel::Sender;
    use std::thread::JoinHandle;

    pub struct Request<A, R> {
        pub responsor: Sender<R>,
        pub arguments: A,
    }

    pub trait Service {
        type Controller;
        // Service's main loop
        fn start<S: ToString>(self, thread_name: S) -> (JoinHandle<()>, Self::Controller);
    }
}

/* ================ */
/* ==== Simple ==== */
/* ================ */
type Task1ResponseValue = u32;
type Task1Arguments = (String, bool);

struct SimpleService {
    pubsub: PubsubController,
    new_tx_receiver: Receiver<()>,
    shared: Arc<RwLock<FnvHashMap<String, Vec<String>>>>,
}

#[derive(Clone)]
struct SimpleController {
    task1_sender: Sender<Request<Task1Arguments, Task1ResponseValue>>,
    task2_sender: Sender<(String, u64)>,
}

impl Service for SimpleService {
    type Controller = SimpleController;

    fn start<S: ToString>(self, thread_name: S) -> (JoinHandle<()>, Self::Controller) {
        let (task1_sender, task1_receiver) = channel::bounded(128);
        let (task2_sender, task2_receiver) = channel::bounded(128);

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
            }).expect("Start dummy service failed");

        (
            join_handle,
            SimpleController {
                task1_sender,
                task2_sender,
            },
        )
    }
}

impl SimpleService {
    pub fn new(
        pubsub: PubsubController,
        new_tx_name: &str,
        shared: Arc<RwLock<FnvHashMap<String, Vec<String>>>>,
    ) -> SimpleService {
        let new_tx_receiver = pubsub.subscribe_net_tx(new_tx_name.to_string());
        SimpleService {
            new_tx_receiver,
            pubsub,
            shared,
        }
    }

    pub fn handle_request(&self, request: Request<Task1Arguments, Task1ResponseValue>) {
        let Request {
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
        println!("new tx received in SimpleService");
    }
}

impl SimpleController {
    pub fn send_request(&self, arguments: Task1Arguments) -> Receiver<Task1ResponseValue> {
        let (sender, receiver) = channel::bounded(1);
        self.task1_sender.send(Request {
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
type MsgSignal = ();
type MsgNewTx = ();
type MsgNewTip = Arc<Vec<u8>>;
type MsgSwitchFork = Arc<Vec<u32>>;
type PubsubRegister<M> = Sender<Request<(String, usize), Receiver<M>>>;

#[derive(Default)]
struct PubsubService {}

#[derive(Clone)]
struct PubsubController {
    signal: Sender<MsgSignal>,
    new_tx_register: PubsubRegister<MsgNewTx>,
    new_tip_register: PubsubRegister<MsgNewTip>,
    switch_fork_register: PubsubRegister<MsgSwitchFork>,
    new_tx_notifier: Sender<MsgNewTx>,
    new_tip_notifier: Sender<MsgNewTip>,
    switch_fork_notifier: Sender<MsgSwitchFork>,
}

impl Service for PubsubService {
    type Controller = PubsubController;

    fn start<S: ToString>(mut self, thread_name: S) -> (JoinHandle<()>, Self::Controller) {
        let (signal_sender, signal_receiver) = channel::bounded::<()>(1);
        let (register1_sender, register1_receiver) = channel::bounded(2);
        let (register2_sender, register2_receiver) = channel::bounded(2);
        let (register3_sender, register3_receiver) = channel::bounded(2);
        let (event1_sender, event1_receiver) = channel::bounded::<MsgNewTx>(128);
        let (event2_sender, event2_receiver) = channel::bounded::<MsgNewTip>(128);
        let (event3_sender, event3_receiver) = channel::bounded::<MsgSwitchFork>(128);

        let mut new_tx_subscribers = FnvHashMap::default();
        let mut new_tip_subscribers = FnvHashMap::default();
        let mut switch_fork_subscribers = FnvHashMap::default();

        let thread_builder = thread::Builder::new().name(thread_name.to_string());
        let join_handle = thread_builder
            .spawn(move || loop {
                select! {
                    recv(signal_receiver, msg) => {
                        break;
                    }

                    recv(register1_receiver, msg) => match msg {
                        Some(Request { responsor, arguments: (name, capacity) }) => {
                            println!("Register event1 {:?}", name);
                            let (sender, receiver) = channel::bounded::<MsgNewTx>(capacity);
                            new_tx_subscribers.insert(name, sender);
                            responsor.send(receiver);
                        },
                        None => println!("Register 1 channel is closed"),
                    }
                    recv(register2_receiver, msg) => match msg {
                        Some(Request { responsor, arguments: (name, capacity)}) => {
                            println!("Register event2 {:?}", name);
                            let (sender, receiver) = channel::bounded::<MsgNewTip>(capacity);
                            new_tip_subscribers.insert(name, sender);
                            responsor.send(receiver);
                        },
                        None => println!("Register 2 channel is closed"),
                    }
                    recv(register3_receiver, msg) => match msg {
                        Some(Request { responsor, arguments: (name, capacity)}) => {
                            println!("Register event3 {:?}", name);
                            let (sender, receiver) = channel::bounded::<MsgSwitchFork>(capacity);
                            switch_fork_subscribers.insert(name, sender);
                            responsor.send(receiver);
                        },
                        None => println!("Register 3 channel is closed"),
                    }

                    recv(event1_receiver, msg) => match msg {
                        Some(msg) => {
                            println!("event new tx {:?}", msg);
                            for (name, subscriber) in &new_tx_subscribers {
                                println!("Send new tx to: {}", name);
                                subscriber.send(msg.clone());
                            }
                        },
                        None => println!("event 1 channel is closed"),
                    }
                    recv(event2_receiver, msg) => match msg {
                        Some(msg) => {
                            println!("event new tip {:?}", msg);
                            for subscriber in new_tip_subscribers.values() {
                                subscriber.send(msg.clone());
                            }
                        },
                        None => println!("event 1 channel is closed"),
                    }
                    recv(event3_receiver, msg) => match msg {
                        Some(msg) => {
                            println!("event switch fork {:?}", msg);
                            for subscriber in switch_fork_subscribers.values() {
                                subscriber.send(msg.clone());
                            }
                        },
                        None => println!("event 3 channel is closed"),
                    }
                }
            }).expect("Start pubsub service failed");

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
    pub fn stop(self) {
        self.signal.send(());
    }

    pub fn subscribe_net_tx<S: ToString>(&self, name: S) -> Receiver<MsgNewTx> {
        let (responsor, response) = channel::bounded(1);
        self.new_tx_register.send(Request {
            responsor,
            arguments: (name.to_string(), 128),
        });
        // Ensure the subscriber is registered.
        response.recv().expect("Subscribe new tx failed")
    }
    pub fn subscribe_net_tip<S: ToString>(&self, name: S) -> Receiver<MsgNewTip> {
        let (responsor, response) = channel::bounded(1);
        self.new_tip_register.send(Request {
            responsor,
            arguments: (name.to_string(), 128),
        });
        // Ensure the subscriber is registered.
        response.recv().expect("Subscribe new tip failed")
    }
    pub fn subscribe_switch_fork<S: ToString>(&self, name: S) -> Receiver<MsgSwitchFork> {
        let (responsor, response) = channel::bounded(1);
        self.switch_fork_register.send(Request {
            responsor,
            arguments: (name.to_string(), 128),
        });
        // Ensure the subscriber is registered.
        response.recv().expect("Subscribe switch fork failed")
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
