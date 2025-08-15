use std::{collections::HashMap, io, str, time::{Duration, Instant}};
use tokio::{net::{TcpListener, TcpStream}, io::{AsyncReadExt, AsyncWriteExt, BufReader}, sync::Mutex, time::sleep};
use bytes::BytesMut;
use std::sync::Arc;

#[derive(Debug, Clone)]
enum Value {
    Bulk(Vec<u8>),
    Integer(i64),
}

#[derive(Debug, Clone)]
struct Entry {
    val: Value,
    expires_at: Option<Instant>,
}

type Db = Arc<Mutex<HashMap<Vec<u8>, Entry>>>;

#[tokio::main]
async fn main() -> io::Result<()> {
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr).await?;
    println!("tinyredis listening on {}", addr);

    let db: Db = Arc::new(Mutex::new(HashMap::new()));

    // background task: active expiry sampling
    {
        let db = db.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(200)).await;
                let mut store = db.lock().await;
                let now = Instant::now();
                let keys: Vec<Vec<u8>> = store
                    .iter()
                    .filter_map(|(k, e)| match e.expires_at { Some(t) if t <= now => Some(k.clone()), _ => None })
                    .take(64)
                    .collect();
                for k in keys { store.remove(&k); }
            }
        });
    }

    loop {
        let (stream, _) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, db).await {
                eprintln!("connection error: {}", e);
            }
        });
    }
}

async fn handle_conn(stream: TcpStream, db: Db) -> io::Result<()> {
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        let frame = match read_resp_array(&mut reader, &mut buf).await? {
            Some(f) => f,
            None => return Ok(()), // client closed
        };
        if frame.is_empty() { continue; }

        let cmd = upper_ascii(&frame[0]);
        let reply = match cmd.as_slice() {
            b"PING" => cmd_ping(&frame[1..]),
            b"ECHO" => cmd_echo(&frame[1..]),
            b"SET"  => cmd_set(&db, &frame[1..]).await,
            b"GET"  => cmd_get(&db, &frame[1..]).await,
            b"DEL"  => cmd_del(&db, &frame[1..]).await,
            b"EXISTS" => cmd_exists(&db, &frame[1..]).await,
            b"INCR" => cmd_incr(&db, &frame[1..]).await,
            b"TTL"  => cmd_ttl(&db, &frame[1..]).await,
            _ => resp_error("ERR unknown command"),
        };

        w.write_all(&reply).await?;
    }
}

fn upper_ascii(s: &Vec<u8>) -> Vec<u8> { s.iter().map(|b| b.to_ascii_uppercase()).collect() }

// ---------------------
// RESP minimal parser
// ---------------------
// We only accept Arrays of Bulk Strings: *N \r\n ($len\r\n data \r\n){N}

async fn read_line(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>, buf: &mut BytesMut) -> io::Result<Option<Vec<u8>>> {
    buf.clear();
    loop {
        let mut byte = [0u8; 1];
        let n = reader.read(&mut byte).await?;
        if n == 0 { // EOF
            if buf.is_empty() { return Ok(None); } else { return Ok(Some(buf.to_vec())); }
        }
        buf.extend_from_slice(&byte);
        let len = buf.len();
        if len >= 2 && &buf[len-2..] == b"\r\n" {
            return Ok(Some(buf[..len-2].to_vec()));
        }
        if buf.len() > 1_048_576 { // 1MB line safety
            return Err(io::Error::new(io::ErrorKind::InvalidData, "line too long"));
        }
    }
}

async fn read_exact_bytes(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>, len: usize) -> io::Result<Vec<u8>> {
    let mut data = vec![0u8; len];
    reader.read_exact(&mut data).await?;
    // consume trailing CRLF
    let mut crlf = [0u8; 2];
    reader.read_exact(&mut crlf).await?;
    if &crlf != b"\r\n" { return Err(io::Error::new(io::ErrorKind::InvalidData, "missing CRLF after bulk")); }
    Ok(data)
}

async fn read_resp_array(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>, scratch: &mut BytesMut) -> io::Result<Option<Vec<Vec<u8>>>> {
    let first = match read_line(reader, scratch).await? { Some(l) => l, None => return Ok(None) };
    if first.is_empty() { return Ok(Some(vec![])); }
    if first[0] != b'*' {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected Array"));
    }
    let n: i64 = str::from_utf8(&first[1..]).unwrap_or("-1").parse().unwrap_or(-1);
    if n < 0 { return Ok(Some(vec![])); }
    let mut items = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let head = read_line(reader, scratch).await?.ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "eof"))?;
        if head.is_empty() || head[0] != b'$' {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "expected Bulk String"));
        }
        let len: i64 = str::from_utf8(&head[1..]).unwrap_or("-1").parse().unwrap_or(-1);
        if len < 0 { items.push(Vec::new()); continue; }
        let data = read_exact_bytes(reader, len as usize).await?;
        items.push(data);
    }
    Ok(Some(items))
}

// ---------------------
// Command implementations
// ---------------------

fn resp_simple(s: &str) -> Vec<u8> { format!("+{}\r\n", s).into_bytes() }
fn resp_bulk(b: &[u8]) -> Vec<u8> { let mut v = format!("${}\r\n", b.len()).into_bytes(); v.extend_from_slice(b); v.extend_from_slice(b"\r\n"); v }
fn resp_nil() -> Vec<u8> { b"$-1\r\n".to_vec() }
fn resp_int(i: i64) -> Vec<u8> { format!(":{}\r\n", i).into_bytes() }
fn resp_error(e: &str) -> Vec<u8> { format!("-{}\r\n", e).into_bytes() }

fn cmd_ping(args: &[Vec<u8>]) -> Vec<u8> {
    if args.is_empty() { resp_simple("PONG") } else { resp_bulk(&args[0]) }
}

fn cmd_echo(args: &[Vec<u8>]) -> Vec<u8> {
    if args.len() != 1 { return resp_error("ERR wrong number of arguments for 'ECHO'"); }
    resp_bulk(&args[0])
}

async fn cmd_set(db: &Db, args: &[Vec<u8>]) -> Vec<u8> {
    if args.len() < 2 { return resp_error("ERR wrong number of arguments for 'SET'"); }
    let key = args[0].clone();
    let val = Value::Bulk(args[1].clone());
    let mut expires: Option<Instant> = None;
    let mut i = 2usize;
    while i < args.len() {
        let opt = upper_ascii(&args[i]);
        if opt.as_slice() == b"EX" {
            if i+1 >= args.len() { return resp_error("ERR syntax error"); }
            let secs = str::from_utf8(&args[i+1]).ok().and_then(|s| s.parse::<u64>().ok());
            if let Some(s) = secs { expires = Some(Instant::now() + Duration::from_secs(s)); i += 2; } else { return resp_error("ERR value is not an integer or out of range"); }
        } else if opt.as_slice() == b"PX" {
            if i+1 >= args.len() { return resp_error("ERR syntax error"); }
            let ms = str::from_utf8(&args[i+1]).ok().and_then(|s| s.parse::<u64>().ok());
            if let Some(ms) = ms { expires = Some(Instant::now() + Duration::from_millis(ms)); i += 2; } else { return resp_error("ERR value is not an integer or out of range"); }
        } else {
            return resp_error("ERR syntax error");
        }
    }
    let mut store = db.lock().await;
    store.insert(key, Entry { val, expires_at: expires });
    resp_simple("OK")
}

async fn cmd_get(db: &Db, args: &[Vec<u8>]) -> Vec<u8> {
    if args.len() != 1 { return resp_error("ERR wrong number of arguments for 'GET'"); }
    let key = &args[0];
    let mut store = db.lock().await;
    match store.get(key) {
        Some(e) => {
            if let Some(t) = e.expires_at { if t <= Instant::now() { store.remove(key); return resp_nil(); } }
            match &e.val { Value::Bulk(b) => resp_bulk(b), Value::Integer(i) => resp_bulk(i.to_string().as_bytes()) }
        },
        None => resp_nil(),
    }
}

async fn cmd_del(db: &Db, args: &[Vec<u8>]) -> Vec<u8> {
    if args.is_empty() { return resp_error("ERR wrong number of arguments for 'DEL'"); }
    let mut store = db.lock().await;
    let mut n = 0;
    for k in args { if store.remove(k).is_some() { n += 1; } }
    resp_int(n)
}

async fn cmd_exists(db: &Db, args: &[Vec<u8>]) -> Vec<u8> {
    if args.is_empty() { return resp_error("ERR wrong number of arguments for 'EXISTS'"); }
    let store = db.lock().await;
    let mut n = 0;
    for k in args {
        if let Some(e) = store.get(k) {
            if e.expires_at.map(|t| t <= Instant::now()).unwrap_or(false) { continue; }
            n += 1;
        }
    }
    resp_int(n)
}

async fn cmd_incr(db: &Db, args: &[Vec<u8>]) -> Vec<u8> {
    if args.len() != 1 { return resp_error("ERR wrong number of arguments for 'INCR'"); }
    let key = args[0].clone();
    let mut store = db.lock().await;
    let now = Instant::now();
    let entry = store.entry(key).or_insert(Entry { val: Value::Integer(0), expires_at: None });
    if let Some(t) = entry.expires_at { if t <= now { entry.val = Value::Integer(0); entry.expires_at = None; } }
    match &mut entry.val {
        Value::Integer(i) => { *i += 1; resp_int(*i) },
        Value::Bulk(b) => match str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(mut v) => { v += 1; entry.val = Value::Integer(v); resp_int(v) },
            None => resp_error("ERR value is not an integer or out of range"),
        },
    }
}

async fn cmd_ttl(db: &Db, args: &[Vec<u8>]) -> Vec<u8> {
    if args.len() != 1 { return resp_error("ERR wrong number of arguments for 'TTL'"); }
    let key = &args[0];
    let mut store = db.lock().await;
    match store.get_mut(key) {
        Some(e) => match e.expires_at {
            Some(t) => {
                let now = Instant::now();
                if t <= now { store.remove(key); resp_int(-2) } else { let secs = ((t - now).as_secs_f64()).ceil() as i64; resp_int(secs) }
            }
            None => resp_int(-1),
        },
        None => resp_int(-2),
    }
}
