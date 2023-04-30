use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::Write;
use tokio::io::AsyncBufReadExt;

use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

type Key = String;
type Val = String;

type Db = HashMap<String, String>;
type SyncDb = Arc<Mutex<Db>>;
type SyncFile = Arc<Mutex<File>>;

#[derive(Debug)]
enum Command {
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
        } else if s.starts_with("DEL") {
            Command::Delete(key)
        } else {
            Command::Unknown
        }
    }
}

async fn handle_client(
    socket: &mut TcpStream,
    file: &mut SyncFile,
    hashmap: &mut SyncDb,
) -> Result<()> {
    let (mut read_stream, _write_stream) = tokio::io::split(socket);
    loop {
        let mut read_stream = BufReader::new(&mut read_stream);
        loop {
            let mut data = String::new();
            let _read = read_stream.read_line(&mut data).await?;
            let data = data.trim_end().to_string();
            dbg!(&data);
            match Command::from(data) {
                Command::Delete(key) => {
                    let mut hashmap = hashmap.lock().unwrap();
                    let mut file = file.lock().unwrap();
                    hashmap.remove(&key);
                    let str_command = format!("DEL {}\n", key);
                    dbg!(&str_command);
                    file.write_all(str_command.as_bytes())?;
                    file.sync_all()?;
                }
                Command::Set(key, val) => {
                    let mut hashmap = hashmap.lock().unwrap();
                    let mut file = file.lock().unwrap();
                    hashmap.insert(key.clone(), val.clone());
                    let str_command = format!("SET {} {}\n", key, val);
                    dbg!(&str_command);
                    file.write_all(str_command.as_bytes())?;
                    file.sync_all()?;
                }
                Command::Unknown => {}
            }
        }
    }
}

fn create_log_file() -> Result<File> {
    Ok(OpenOptions::new()
        .append(true)
        .create(true)
        .open("follower.db")?)
}

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
            Command::Unknown => {}
        }
    }
    Ok(hashmap)
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("localhost:48000").await?;
    let mut hashmap = HashMap::default();
    if let Ok(file) = OpenOptions::new().read(true).open("follower.db") {
        hashmap = replay(file)?;
    };
    let log_file = create_log_file()?;
    let file = Arc::new(Mutex::new(log_file));
    let hashmap = Arc::new(Mutex::new(hashmap));

    dbg!(&hashmap);

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
