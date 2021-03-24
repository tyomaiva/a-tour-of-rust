use clap::{load_yaml, App};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::net::UdpSocket;
use std::{thread, time};

#[derive(Deserialize)]
struct ExternalCommand {
    command_name: String,
    params: HashMap<String, Value>,
}

fn worker_task(command_str: String) {
    let command: ExternalCommand =
        serde_json::from_str(command_str.as_str()).expect("Can't continue without proper data!");
    println!("The command is {}", command.command_name);
    let time_to_wait = command
        .params
        .get("amount_of_time_to_wait")
        .expect("The time parameter is required!")
        .as_u64()
        .unwrap();

    let current_thread_index = rayon::current_thread_index().unwrap_or(0);

    println!(
        "Thread#: {}, time to wait: {}",
        current_thread_index, &time_to_wait
    );
    let sleep_duration = time::Duration::from_secs(time_to_wait);
    thread::sleep(sleep_duration);
    println!("Command executed!");
}

fn serve(n_threads: usize, port: String) {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(n_threads)
        .build()
        .unwrap();

    let remote = format!("127.0.0.1:{}", port);
    let socket = UdpSocket::bind(remote).expect("Couldn't create a socket");

    let mut buf = [0u8; 1024];

    loop {
        match socket.recv(&mut buf) {
            Ok(received) => {
                let received_str =
                    String::from_utf8((&buf[..received]).to_vec()).unwrap_or(String::from("{}"));
                println!("received {} bytes: {}", received, &received_str);
                pool.spawn(move || worker_task(received_str));
            }
            Err(e) => {
                println!("recv function failed: {:?}", e);
                break;
            }
        }
        buf = [0u8; 1024];
    }
    println!("Bye");
}

fn main() {
    let yaml = load_yaml!("options.yaml");
    let matches = App::from(yaml).get_matches();

    let port_str = matches.value_of("port").unwrap();
    let n_worker = matches
        .value_of("n_worker")
        .unwrap_or("4")
        .parse::<usize>()
        .expect("Please provide a valid number for the number of threads!");

    serve(n_worker, String::from(port_str));
}
