use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;

use clap::Parser;
use rand::Rng;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "ipv6-rotator", about = "HTTP CONNECT proxy with IPv6 rotation")]
struct Args {
    /// IPv6 prefix in CIDR notation (e.g. 2607:fea8:5765:bc01::/64)
    #[arg(long)]
    prefix: String,

    /// Network interface (used only by --setup to add addresses)
    #[arg(long, default_value = "eth0")]
    interface: String,

    /// Listen port
    #[arg(long, default_value_t = 1080)]
    port: u16,

    /// Listen address
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// Number of IPv6 addresses to generate
    #[arg(long, default_value_t = 200)]
    pool_size: usize,

    /// Run `ip -6 addr add` for each address (requires root)
    #[arg(long)]
    setup: bool,
}

// ---------------------------------------------------------------------------
// IPv6 pool generation
// ---------------------------------------------------------------------------

/// Parse a CIDR prefix like `2607:fea8:5765:bc01::/64` into (base_addr, prefix_len).
fn parse_prefix(s: &str) -> Result<(u128, u8), String> {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() != 2 {
        return Err(format!("invalid CIDR: {s}"));
    }
    let addr: Ipv6Addr = parts[0]
        .parse()
        .map_err(|e| format!("bad IPv6 address: {e}"))?;
    let prefix_len: u8 = parts[1]
        .parse()
        .map_err(|e| format!("bad prefix length: {e}"))?;
    if prefix_len > 128 {
        return Err("prefix length must be <= 128".into());
    }
    Ok((u128::from(addr), prefix_len))
}

/// Generate `count` random IPv6 addresses within the given prefix.
fn generate_pool(base: u128, prefix_len: u8, count: usize) -> Vec<Ipv6Addr> {
    let host_bits = 128 - prefix_len as u32;
    // Mask out the host portion of the base address.
    let network = if host_bits == 128 {
        0u128
    } else {
        base & (!0u128 << host_bits)
    };

    let mut rng = rand::thread_rng();
    let mut addrs = Vec::with_capacity(count);

    for _ in 0..count {
        let suffix: u128 = if host_bits >= 128 {
            rng.gen()
        } else if host_bits == 0 {
            0
        } else {
            rng.gen::<u128>() & ((1u128 << host_bits) - 1)
        };
        // Avoid the all-zeros suffix (network address).
        let suffix = if suffix == 0 { 1 } else { suffix };
        addrs.push(Ipv6Addr::from(network | suffix));
    }
    addrs
}

/// Abbreviate an IPv6 address for log output (first two and last two hextets).
fn abbrev_ipv6(addr: &Ipv6Addr) -> String {
    let seg = addr.segments();
    format!("{:x}:{:x}::…::{:x}:{:x}", seg[0], seg[1], seg[6], seg[7])
}

// ---------------------------------------------------------------------------
// Setup helper — add addresses to the system
// ---------------------------------------------------------------------------

async fn setup_addresses(addrs: &[Ipv6Addr], interface: &str) {
    for addr in addrs {
        let output = tokio::process::Command::new("ip")
            .args(["-6", "addr", "add", &format!("{addr}/128"), "dev", interface])
            .output()
            .await;
        match output {
            Ok(o) if o.status.success() => {}
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                // "RTNETLINK answers: File exists" is fine — address already present.
                if !stderr.contains("File exists") {
                    eprintln!("  warn: ip addr add {addr}: {stderr}");
                }
            }
            Err(e) => eprintln!("  warn: failed to run `ip`: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Proxy core
// ---------------------------------------------------------------------------

/// Connect to `target` using a randomly chosen source IPv6 from the pool.
async fn connect_via_ipv6(
    target: &str,
    pool: &[Ipv6Addr],
) -> io::Result<TcpStream> {
    // Resolve the target address.
    let target_addrs: Vec<SocketAddr> = tokio::net::lookup_host(target).await?.collect();
    if target_addrs.is_empty() {
        return Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "DNS returned no addresses"));
    }

    // Pick a random source IPv6.
    let src = pool[rand::thread_rng().gen_range(0..pool.len())];

    // Try each resolved address; prefer IPv6 targets, fall back to IPv4.
    // For IPv4 targets we cannot bind an IPv6 source, so connect without binding.
    let mut last_err: Option<io::Error> = None;

    // Sort: IPv6 first.
    let mut sorted = target_addrs.clone();
    sorted.sort_by_key(|a| if a.is_ipv6() { 0 } else { 1 });

    for addr in &sorted {
        if addr.is_ipv6() {
            match connect_with_bind(src, *addr).await {
                Ok(stream) => {
                    eprintln!("  -> {addr} via {}", abbrev_ipv6(&src));
                    return Ok(stream);
                }
                Err(e) => last_err = Some(e),
            }
        } else {
            // IPv4 target — cannot use IPv6 source, connect directly.
            match TcpStream::connect(addr).await {
                Ok(stream) => {
                    eprintln!("  -> {addr} (IPv4, no rotation)");
                    return Ok(stream);
                }
                Err(e) => last_err = Some(e),
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::ConnectionRefused, "all addresses failed")
    }))
}

/// Create a TCP socket, bind it to `src_ipv6`, and connect to `target`.
async fn connect_with_bind(src: Ipv6Addr, target: SocketAddr) -> io::Result<TcpStream> {
    let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_nonblocking(true)?;
    // Allow binding to addresses not yet fully configured (DAD pending, etc.).
    // This is Linux-specific: IP_FREEBIND.
    #[cfg(target_os = "linux")]
    socket.set_freebind(true)?;

    let src_sa = SockAddr::from(SocketAddrV6::new(src, 0, 0, 0));
    socket.bind(&src_sa)?;

    let target_sa = SockAddr::from(target);
    // socket2 connect on a nonblocking socket returns WouldBlock / InProgress.
    match socket.connect(&target_sa) {
        Ok(()) => {}
        Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
        Err(e) => return Err(e),
    }

    let std_stream: std::net::TcpStream = socket.into();
    let stream = TcpStream::from_std(std_stream)?;

    // Wait for the connection to actually complete.
    stream.writable().await?;
    // Check for a connect error.
    if let Some(e) = stream.take_error()? {
        return Err(e);
    }
    Ok(stream)
}

// ---------------------------------------------------------------------------
// HTTP parsing helpers
// ---------------------------------------------------------------------------

/// Read headers from the client, returning the raw bytes consumed.
/// We read byte-by-byte until we see \r\n\r\n.
async fn read_http_head(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(4096);
    loop {
        let byte = stream.read_u8().await?;
        buf.push(byte);
        if buf.len() >= 4 && &buf[buf.len() - 4..] == b"\r\n\r\n" {
            return Ok(buf);
        }
        if buf.len() > 8192 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request header too large",
            ));
        }
    }
}

/// Parse the first line of an HTTP request.
/// Returns (method, target, version).
fn parse_request_line(head: &[u8]) -> Option<(&str, &str, &str)> {
    let first_line_end = head.windows(2).position(|w| w == b"\r\n")?;
    let first_line = std::str::from_utf8(&head[..first_line_end]).ok()?;
    let mut parts = first_line.splitn(3, ' ');
    let method = parts.next()?;
    let target = parts.next()?;
    let version = parts.next()?;
    Some((method, target, version))
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_client(mut client: TcpStream, peer: SocketAddr, pool: Arc<Vec<Ipv6Addr>>) {
    let head = match read_http_head(&mut client).await {
        Ok(h) => h,
        Err(e) => {
            eprintln!("[{peer}] read error: {e}");
            return;
        }
    };

    let (method, target, _version) = match parse_request_line(&head) {
        Some(t) => t,
        None => {
            eprintln!("[{peer}] malformed request");
            let _ = client.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
            return;
        }
    };

    if method.eq_ignore_ascii_case("CONNECT") {
        handle_connect(client, peer, target, &pool).await;
    } else {
        handle_plain_http(client, peer, method, target, &head, &pool).await;
    }
}

/// Handle HTTP CONNECT tunneling (used by HTTPS_PROXY).
async fn handle_connect(
    mut client: TcpStream,
    peer: SocketAddr,
    target: &str,
    pool: &[Ipv6Addr],
) {
    eprintln!("[{peer}] CONNECT {target}");

    // target is host:port
    let mut upstream = match connect_via_ipv6(target, pool).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[{peer}]   error: {e}");
            let _ = client
                .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                .await;
            return;
        }
    };

    // Tell the client the tunnel is established.
    if client
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await
        .is_err()
    {
        return;
    }

    let (mut cr, mut cw) = io::split(client);
    let (mut ur, mut uw) = io::split(upstream);

    tokio::select! {
        res = io::copy(&mut cr, &mut uw) => {
            let bytes = res.unwrap_or(0);
            let _ = uw.shutdown().await;
            eprintln!("[{peer}]   done  client>{bytes}");
        }
        res = io::copy(&mut ur, &mut cw) => {
            let bytes = res.unwrap_or(0);
            let _ = cw.shutdown().await;
            eprintln!("[{peer}]   done  server>{bytes}");
        }
    }
}

/// Handle plain HTTP proxy requests (GET http://example.com/...).
async fn handle_plain_http(
    mut client: TcpStream,
    peer: SocketAddr,
    method: &str,
    url: &str,
    head: &[u8],
    pool: &[Ipv6Addr],
) {
    // Parse the absolute URL to extract host and port.
    let stripped = url
        .strip_prefix("http://")
        .unwrap_or(url);
    let (host_port, path) = match stripped.find('/') {
        Some(i) => (&stripped[..i], &stripped[i..]),
        None => (stripped, "/"),
    };

    let target = if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{host_port}:80")
    };

    eprintln!("[{peer}] {method} {url}");

    let mut upstream = match connect_via_ipv6(&target, pool).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[{peer}]   error: {e}");
            let _ = client
                .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                .await;
            return;
        }
    };

    // Rebuild the request with a relative path instead of the absolute URL.
    let first_line_end = head
        .windows(2)
        .position(|w| w == b"\r\n")
        .unwrap_or(head.len());
    let rest_of_headers = &head[first_line_end..]; // includes the \r\n

    let new_request_line = format!("{method} {path} HTTP/1.1");
    let mut forwarded = Vec::with_capacity(new_request_line.len() + rest_of_headers.len());
    forwarded.extend_from_slice(new_request_line.as_bytes());
    forwarded.extend_from_slice(rest_of_headers);

    if upstream.write_all(&forwarded).await.is_err() {
        let _ = client
            .write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            .await;
        return;
    }

    let (mut cr, mut cw) = io::split(client);
    let (mut ur, mut uw) = io::split(upstream);

    tokio::select! {
        res = io::copy(&mut cr, &mut uw) => {
            let bytes = res.unwrap_or(0);
            let _ = uw.shutdown().await;
            eprintln!("[{peer}]   done  client>{bytes}");
        }
        res = io::copy(&mut ur, &mut cw) => {
            let bytes = res.unwrap_or(0);
            let _ = cw.shutdown().await;
            eprintln!("[{peer}]   done  server>{bytes}");
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let (base, prefix_len) = parse_prefix(&args.prefix)?;
    let pool = generate_pool(base, prefix_len, args.pool_size);

    eprintln!("=== ipv6-rotator ===");
    eprintln!("prefix:     {}", args.prefix);
    eprintln!("pool size:  {}", pool.len());
    eprintln!("listen:     {}:{}", args.bind, args.port);
    eprintln!();

    // Print generated addresses (for manual setup or piping to a script).
    eprintln!("--- generated addresses ---");
    for addr in &pool {
        println!("{addr}");
    }
    eprintln!("--- end ---");
    eprintln!();

    // Optionally add addresses to the interface.
    if args.setup {
        eprintln!("running --setup: adding addresses to {}", args.interface);
        setup_addresses(&pool, &args.interface).await;
        eprintln!("setup complete");
        eprintln!();
    }

    let pool = Arc::new(pool);
    let listen_addr = format!("{}:{}", args.bind, args.port);
    let listener = TcpListener::bind(&listen_addr).await?;
    eprintln!("listening on {listen_addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        let pool = Arc::clone(&pool);
        tokio::spawn(async move {
            handle_client(stream, peer, pool).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_prefix_64() {
        let (base, len) = parse_prefix("2607:fea8:5765:bc01::/64").unwrap();
        assert_eq!(len, 64);
        let addr = Ipv6Addr::from(base);
        assert_eq!(addr, "2607:fea8:5765:bc01::".parse::<Ipv6Addr>().unwrap());
    }

    #[test]
    fn test_parse_prefix_48() {
        let (_, len) = parse_prefix("2001:db8:abcd::/48").unwrap();
        assert_eq!(len, 48);
    }

    #[test]
    fn test_generate_pool_within_prefix() {
        let (base, prefix_len) = parse_prefix("2607:fea8:5765:bc01::/64").unwrap();
        let pool = generate_pool(base, prefix_len, 50);
        assert_eq!(pool.len(), 50);

        let network_mask = !0u128 << (128 - 64);
        let expected_network = base & network_mask;
        for addr in &pool {
            let a = u128::from(*addr);
            assert_eq!(
                a & network_mask,
                expected_network,
                "address {addr} not in prefix"
            );
        }
    }

    #[test]
    fn test_generate_pool_no_zero_suffix() {
        let (base, prefix_len) = parse_prefix("2607:fea8:5765:bc01::/64").unwrap();
        let pool = generate_pool(base, prefix_len, 500);
        let network_mask = !0u128 << (128 - 64);
        for addr in &pool {
            let a = u128::from(*addr);
            let suffix = a & !network_mask;
            assert_ne!(suffix, 0, "got network address in pool");
        }
    }

    #[test]
    fn test_abbrev_ipv6() {
        let addr: Ipv6Addr = "2607:fea8:5765:bc01:1234:5678:9abc:def0".parse().unwrap();
        let s = abbrev_ipv6(&addr);
        assert_eq!(s, "2607:fea8::…::9abc:def0");
    }

    #[test]
    fn test_parse_request_line_connect() {
        let head = b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n";
        let (method, target, version) = parse_request_line(head).unwrap();
        assert_eq!(method, "CONNECT");
        assert_eq!(target, "example.com:443");
        assert_eq!(version, "HTTP/1.1");
    }

    #[test]
    fn test_parse_request_line_get() {
        let head = b"GET http://example.com/path HTTP/1.1\r\nHost: example.com\r\n\r\n";
        let (method, target, version) = parse_request_line(head).unwrap();
        assert_eq!(method, "GET");
        assert_eq!(target, "http://example.com/path");
        assert_eq!(version, "HTTP/1.1");
    }

    #[test]
    fn test_parse_prefix_invalid() {
        assert!(parse_prefix("not-a-prefix").is_err());
        assert!(parse_prefix("2001:db8::/200").is_err());
        assert!(parse_prefix("garbage/64").is_err());
    }
}
