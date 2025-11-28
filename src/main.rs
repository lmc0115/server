use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;

// ================== Data structures ==================

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Song {
    id: u64,
    title: String,
    artist: String,
    genre: String,
    play_count: u64,
}

#[derive(Serialize, Deserialize)]
struct Library {
    next_id: u64,
    songs: Vec<Song>,
}

impl Default for Library {
    fn default() -> Self {
        Library {
            next_id: 1,
            songs: Vec::new(),
        }
    }
}

// ================== Global state ==================

static LIBRARY: OnceLock<Mutex<Library>> = OnceLock::new();
static VISIT_COUNT: AtomicU64 = AtomicU64::new(0); // atomic for concurrent /count

fn library() -> &'static Mutex<Library> {
    LIBRARY.get_or_init(|| {
        let lib = load_library_from_disk().unwrap_or_default();
        Mutex::new(lib)
    })
}

// ================== Persistence ==================

const LIB_FILE: &str = "library.json";

fn load_library_from_disk() -> Option<Library> {
    if let Ok(contents) = fs::read_to_string(LIB_FILE) {
        serde_json::from_str(&contents).ok()
    } else {
        None
    }
}

fn save_library_to_disk(lib: &Library) {
    if let Ok(json) = serde_json::to_string(lib) {
        let _ = fs::write(LIB_FILE, json);
    }
}

// ================== Small helpers ==================

fn respond(mut stream: TcpStream, status: &str, body: &str, content_type: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.as_bytes().len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
}

fn url_decode_plus(s: &str) -> String {
    // simple decoding: '+' -> ' '
    s.replace('+', " ")
}

fn parse_query(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut it = pair.splitn(2, '=');
        let key = it.next().unwrap_or("").to_string();
        let value_raw = it.next().unwrap_or("");
        let value = url_decode_plus(value_raw);
        map.insert(key, value);
    }
    map
}

// ================== Library operations ==================

fn add_song(json_body: &str) -> Option<Song> {
    #[derive(Deserialize)]
    struct NewSong {
        title: String,
        artist: String,
        genre: String,
    }

    let new: NewSong = serde_json::from_str(json_body).ok()?;
    let mut lib = library().lock().unwrap();

    let song = Song {
        id: lib.next_id,
        title: new.title,
        artist: new.artist,
        genre: new.genre,
        play_count: 0,
    };
    lib.next_id += 1;
    lib.songs.push(song.clone());
    save_library_to_disk(&lib);
    Some(song)
}

fn search_songs(query: &str) -> Vec<Song> {
    let params = parse_query(query);

    let title_q = params.get("title").map(|s| s.to_lowercase());
    let artist_q = params.get("artist").map(|s| s.to_lowercase());
    let genre_q = params.get("genre").map(|s| s.to_lowercase());

    let lib = library().lock().unwrap();

    lib.songs
        .iter()
        .cloned()
        .filter(|song| {
            if let Some(ref q) = title_q {
                if !song.title.to_lowercase().contains(q) {
                    return false;
                }
            }
            if let Some(ref q) = artist_q {
                if !song.artist.to_lowercase().contains(q) {
                    return false;
                }
            }
            if let Some(ref q) = genre_q {
                if !song.genre.to_lowercase().contains(q) {
                    return false;
                }
            }
            true
        })
        .collect()
}

fn play_song(id: u64) -> Option<Song> {
    let mut lib = library().lock().unwrap();
    if let Some(song) = lib.songs.iter_mut().find(|s| s.id == id) {
        song.play_count += 1;
        let cloned = song.clone();
        save_library_to_disk(&lib);
        Some(cloned)
    } else {
        None
    }
}

// ================== Request handling ==================

fn handle_connection(mut stream: TcpStream) {
    let mut buf = [0u8; 2048];

    let bytes_read = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };

    let buffer = String::from_utf8_lossy(&buf[..bytes_read]).to_string();

    // request line: "GET /path?query HTTP/1.1"
    let mut lines = buffer.lines();
    let request_line = match lines.next() {
        Some(l) => l,
        None => return,
    };

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return;
    }
    let method = parts[0];
    let full_path = parts[1];

    let (path, query) = if let Some(pos) = full_path.find('?') {
        (&full_path[..pos], Some(&full_path[pos + 1..]))
    } else {
        (full_path, None)
    };

    // body after blank line (for POST /songs/new)
    let body = buffer.split("\r\n\r\n").nth(1).unwrap_or("").trim();

    match (method, path) {
        ("GET", "/") => {
            respond(
                stream,
                "200 OK",
                "Welcome to the Rust-powered web server!",
                "text/plain",
            );
        }

        ("GET", "/count") => {
            // relaxed ordering is enough; no other shared data depends on it
            let new_count = VISIT_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
            let body = format!("Visit count: {}", new_count);
            respond(stream, "200 OK", &body, "text/plain");
        }

        ("POST", "/songs/new") => {
            if let Some(song) = add_song(body) {
                let json = serde_json::to_string(&song).unwrap();
                respond(stream, "200 OK", &json, "application/json");
            } else {
                let json = json!({ "error": "Invalid JSON" }).to_string();
                respond(stream, "400 Bad Request", &json, "application/json");
            }
        }

        ("GET", "/songs/search") => {
            let q = query.unwrap_or("");
            let result = search_songs(q);
            let json = serde_json::to_string(&result).unwrap();
            respond(stream, "200 OK", &json, "application/json");
        }

        _ if method == "GET" && path.starts_with("/songs/play/") => {
            let id_str = &path["/songs/play/".len()..];
            if let Ok(id) = id_str.parse::<u64>() {
                if let Some(song) = play_song(id) {
                    let json = serde_json::to_string(&song).unwrap();
                    respond(stream, "200 OK", &json, "application/json");
                } else {
                    let json = json!({ "error": "Song not found" }).to_string();
                    respond(stream, "404 Not Found", &json, "application/json");
                }
            } else {
                let json = json!({ "error": "Invalid id" }).to_string();
                respond(stream, "400 Bad Request", &json, "application/json");
            }
        }

        _ => {
            let json = json!({ "error": "Not found" }).to_string();
            respond(stream, "404 Not Found", &json, "application/json");
        }
    }
}

// ================== Tiny thread pool ==================

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    NewJob(Job),
    Terminate,
}

struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

impl ThreadPool {
    fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel::<Message>();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);
        for _ in 0..size {
            workers.push(Worker::new(Arc::clone(&receiver)));
        }

        ThreadPool { workers, sender }
    }

    fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        let _ = self.sender.send(Message::NewJob(job));
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // tell workers to stop
        for _ in &self.workers {
            let _ = self.sender.send(Message::Terminate);
        }

        // join all worker threads
        for worker in &mut self.workers {
            if let Some(handle) = worker.thread.take() {
                let _ = handle.join();
            }
        }
    }
}

struct Worker {
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = match receiver.lock() {
                Ok(rx) => rx.recv(),
                Err(_) => break,
            };

            match message {
                Ok(Message::NewJob(job)) => {
                    job();
                }
                Ok(Message::Terminate) | Err(_) => {
                    break;
                }
            }
        });

        Worker {
            thread: Some(thread),
        }
    }
}

// ================== main ==================

fn main() {
    println!("The server is currently listening on localhost:8080.");

    let listener = TcpListener::bind("127.0.0.1:8080").expect("bind failed");

    let num_threads = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let pool = ThreadPool::new(num_threads);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                pool.execute(|| handle_connection(stream));
            }
            Err(_) => {
                // ignore failed connections
            }
        }
    }
}
