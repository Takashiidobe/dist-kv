use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex};

use tokio::io::AsyncWriteExt;

use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};

type Key = String;
type Val = String;

type Db = HashMap<String, String>;

#[derive(Debug)]
enum Command {
    Get(Key),
    Set(Key, Val),
    Delete(Key),
    Unknown,
}

impl From<String> for Command {
    fn from(s: String) -> Self {
        let mut split_s = s.split_whitespace().skip(1);
        let key = split_s.next().expect("Expected a key").to_string();
        if s.starts_with("SET") {
            let val = split_s.next().expect("Expected a value").to_string();
            Command::Set(key, val)
        } else if s.starts_with("GET") {
            Command::Get(key)
        } else if s.starts_with("DEL") {
            Command::Delete(key)
        } else {
            Command::Unknown
        }
    }
}

#[derive(Debug)]
enum Response {
    Get(Key, Val),
    Set(Key, Val),
    Replace(Key, Val, Val),
    Delete(Key, Val),
    KeyNotFound(Key),
    Unknown,
}

use std::fmt;

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Response::Get(key, val) => write!(f, "Key {}={}", key, val),
            Response::Set(key, val) => write!(f, "Set {}={}", key, val),
            Response::Replace(key, old_val, new_val) => {
                write!(f, "Key {}={}, used to be {}", key, new_val, old_val)
            }
            Response::Delete(key, val) => write!(f, "Deleted key {} that was set to {}", key, val),
            Response::KeyNotFound(key) => write!(f, "Key {} was not found.", key),
            Response::Unknown => write!(f, "Unknown command"),
        }
    }
}

pub fn create_log_file(path: &str) -> Result<File> {
    Ok(OpenOptions::new().append(true).create(true).open(path)?)
}

fn run_command(hashmap: &mut Db, command: &Command) -> Response {
    match command {
        Command::Get(key) => {
            if hashmap.contains_key(key) {
                Response::Get(key.to_string(), hashmap.get(key).unwrap().to_string())
            } else {
                Response::KeyNotFound(key.to_string())
            }
        }
        Command::Set(key, val) => {
            if hashmap.contains_key(key) {
                let old_val = hashmap.get(key).unwrap().to_string();
                let (key, val) = (key.to_string(), val.to_string());
                hashmap.insert(key.clone(), val.clone());
                Response::Replace(key, old_val, val)
            } else {
                let (key, val) = (key.to_string(), val.to_string());
                hashmap.insert(key.to_string(), val.to_string());
                Response::Set(key, val)
            }
        }
        Command::Delete(key) => {
            if hashmap.contains_key(key) {
                let old_val = hashmap.get(key).unwrap().to_string();
                hashmap.remove(key);
                Response::Delete(key.to_string(), old_val)
            } else {
                Response::KeyNotFound(key.to_string())
            }
        }
        Command::Unknown => Response::Unknown,
    }
}

async fn persist_command(
    file: &mut File,
    hashmap: &mut Db,
    stream: &mut TcpStream,
    command: &Command,
) -> Result<()> {
    let response = run_command(hashmap, command);
    match &response {
        Response::Set(key, val) => {
            let str_command = format!("SET {} {}\n", key, val);
            file.write_all(str_command.as_bytes())?;
            stream.write_all(str_command.as_bytes()).await?;
        }
        Response::Delete(key, _val) => {
            let str_command = format!("DEL {}\n", key);
            file.write_all(str_command.as_bytes())?;
            stream.write_all(str_command.as_bytes()).await?;
        }
        _ => {}
    }
    file.sync_all()?;
    println!("{}", response);
    Ok(())
}

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

fn replay(file: File) -> Result<HashMap<String, String>> {
    let mut hashmap = HashMap::default();
    for line in std::io::BufReader::new(file).lines() {
        let line = line?;
        match Command::from(line) {
            Command::Set(key, val) => {
                hashmap.insert(key, val);
            }
            Command::Delete(key) => {
                hashmap.remove(&key);
            }
            _ => {}
        }
    }
    Ok(hashmap)
}

use nix::unistd::{fork, ForkResult};
mod follower;
use follower::*;

async fn setup_follower() -> Result<()> {
    let listener = TcpListener::bind("localhost:48000").await?;
    let mut hashmap = HashMap::default();
    if let Ok(file) = OpenOptions::new().read(true).open("follower.db") {
        hashmap = replay(file)?;
    };
    let log_file = create_log_file("follower.log")?;
    let file = Arc::new(Mutex::new(log_file));
    let hashmap = Arc::new(Mutex::new(hashmap));

    loop {
        let (mut socket, _addr) = listener.accept().await?;
        let mut hashmap = hashmap.clone();
        let mut file = file.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(&mut socket, &mut file, &mut hashmap).await {
                eprintln!("Error = {:?}", e);
            }
        });
    }
}

async fn setup_leader() -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    let mut stream = TcpStream::connect("localhost:48000").await?;

    let mut hashmap = HashMap::default();
    if let Ok(file) = OpenOptions::new().read(true).open("leader.db") {
        hashmap = replay(file)?;
    };

    let mut file = create_log_file("leader.log")?;

    dbg!(&hashmap);

    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                let command = Command::from(line);
                persist_command(&mut file, &mut hashmap, &mut stream, &command).await?;
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                stream.shutdown().await?;
                break;
            }
            Err(err) => {
                stream.shutdown().await?;
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    match unsafe { fork() } {
        Ok(ForkResult::Parent { .. }) => {}
        Ok(ForkResult::Child) => {
            setup_follower().await?;
        }
        Err(_) => println!("Fork failed"),
    }
    setup_leader().await
}
