#[macro_use]
extern crate crossbeam_channel as channel;

use std::thread;
use std::time::Duration;

fn main() {
    let dummy_controller = DummyWorker::default().start();
    let response = dummy_controller.send_request(("this is a request/response task".to_string(), true));
    println!("Got response: {:?}", response.recv());
    dummy_controller.send_event(("this is event to send".to_string(), 333));

    if let Some(join_handle) = dummy_controller.join_handle {
        join_handle.join().expect("Join dummy worker failed");
    }
    println!(">>> DONE!");
}


type Task1ResponseValue = u32;
type Task1Arguments = (String, bool);

type Task2Arguments = (String, u64);

struct RequestTask<A, R> {
    responsor: channel::Sender<R>,
    arguments: A,
}

#[derive(Debug)]
struct EventTask<A> {
    arguments: A,
}

#[derive(Default)]
struct DummyWorker {}


struct DummyWorkerController {
    join_handle: Option<thread::JoinHandle<()>>,
    task1_sender: channel::Sender<RequestTask<Task1Arguments, Task1ResponseValue>>,
    task2_sender: channel::Sender<EventTask<Task2Arguments>>,
}


pub trait Worker {
    type Controller;
    // Worker's main loop
    fn start(self) -> Self::Controller;
}


pub trait WorkerController {
    type EventArguments;
    type RequestArguments;
    type ResponseValue;

    fn send_request(&self, arguments: Self::RequestArguments) -> channel::Receiver<Self::ResponseValue> {
        unimplemented!();
    }

    fn send_event(&self, arguments: Self::EventArguments) {
        unimplemented!();
    }
}


impl Worker for DummyWorker {
    type Controller = DummyWorkerController;

    fn start(self) -> Self::Controller {
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

        DummyWorkerController {
            join_handle: Some(join_handle),
            task1_sender,
            task2_sender,
        }
    }
}

impl WorkerController for DummyWorkerController {
    type EventArguments = Task2Arguments;
    type RequestArguments = Task1Arguments;
    type ResponseValue = Task1ResponseValue;

    fn send_request(&self, arguments: Self::RequestArguments) -> channel::Receiver<Self::ResponseValue> {
        let (sender, receiver) = channel::bounded(1);
        self.task1_sender.send(RequestTask {
            responsor: sender,
            arguments,
        });
        receiver
    }

    fn send_event(&self, arguments: Self::EventArguments) {
        self.task2_sender.send(EventTask { arguments });
    }
}
