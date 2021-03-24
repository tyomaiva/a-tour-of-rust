use clap::{load_yaml, App};
use std::fs;
use std::net::UdpSocket;

fn main() {
    // Compile-time parsing of the YAML file!
    let yaml = load_yaml!("cli_params.yaml");
    let matches = App::from(yaml).get_matches();

    let ip_addr = matches.value_of("ip").unwrap_or("127.0.0.1");
    let port_str = matches
        .value_of("port")
        .expect("Port can't be none it is required");
    let file_path = matches
        .value_of("file")
        .expect("File path can't be none it is required");

    let send_addr = format!("{}:{}", ip_addr, port_str);

    println!(
        "ipaddr : {} port : {}  file : {}",
        ip_addr, port_str, file_path
    );

    let contents = fs::read_to_string(file_path).expect("Something went wrong reading the file");
    println!("The file contents is: \n{}", contents);

    let socket = UdpSocket::bind("0.0.0.0:0").expect("Couldn't create a socket");
    socket
        .send_to(contents.as_bytes(), send_addr)
        .expect("Couldn't send to address");

    println!("Done !")
}
