//! A scripted SSE server for subscription tests.
//!
//! Each accepted connection plays the next [`Connection`] of the script. Once the script runs out, the last
//! connection is replayed, so a test can describe "fail twice, then stay healthy" without knowing how often the
//! client reconnects.

use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    spawn,
    task::JoinHandle,
    time::sleep,
};

/// One action the server takes on an open SSE response.
#[derive(Clone, Debug)]
pub(crate) enum Step {
    /// Writes raw SSE text, such as `"data: {...}\n\n"` or `": keep-alive\n\n"`.
    Send(String),
    /// Waits before the next step.
    Wait(Duration),
    /// Keeps the connection open without sending anything until the test ends.
    Hang,
}

/// How the server answers one connection. The response ends (EOF) after the last step.
#[derive(Clone, Debug)]
pub(crate) struct Connection {
    status: u16,
    steps: Vec<Step>,
}

impl Connection {
    /// A `200 text/event-stream` response that plays `steps`.
    pub(crate) fn ok(steps: Vec<Step>) -> Self {
        Self { status: 200, steps }
    }
}

/// An SSE event carrying `data` as its payload.
pub(crate) fn event(data: &str) -> Step {
    Step::Send(format!("data: {data}\n\n"))
}

/// An SSE comment, as sent by Blokli's keepalive.
pub(crate) fn keepalive() -> Step {
    Step::Send(": keep-alive\n\n".into())
}

pub(crate) struct SseTestServer {
    url: url::Url,
    connections: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl SseTestServer {
    pub(crate) async fn start(script: Vec<Connection>) -> anyhow::Result<Self> {
        anyhow::ensure!(!script.is_empty(), "the script needs at least one connection");
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/", listener.local_addr()?).parse()?;
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let task = spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let index = accepted.fetch_add(1, Ordering::SeqCst);
                let connection = script[index.min(script.len() - 1)].clone();
                spawn(async move {
                    let _ = serve(socket, connection).await;
                });
            }
        });
        Ok(Self { url, connections, task })
    }

    /// Base URL to give to [`BlokliClient`](crate::BlokliClient).
    pub(crate) fn url(&self) -> url::Url {
        self.url.clone()
    }

    /// Number of connections accepted so far.
    pub(crate) fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

impl Drop for SseTestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(mut socket: TcpStream, connection: Connection) -> anyhow::Result<()> {
    read_request(&mut socket).await?;
    let head = format!(
        "HTTP/1.1 {} Test\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n",
        connection.status
    );
    socket.write_all(head.as_bytes()).await?;
    socket.flush().await?;
    for step in connection.steps {
        match step {
            Step::Send(text) => {
                socket.write_all(text.as_bytes()).await?;
                socket.flush().await?;
            }
            Step::Wait(duration) => sleep(duration).await,
            Step::Hang => pending::<()>().await,
        }
    }
    socket.shutdown().await?;
    Ok(())
}

/// Reads the request head and its body, so the client has finished sending before the response starts.
async fn read_request(socket: &mut TcpStream) -> anyhow::Result<()> {
    let mut request = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let read = socket.read(&mut chunk).await?;
        anyhow::ensure!(read > 0, "connection closed before the request head ended");
        request.extend_from_slice(&chunk[..read]);
        if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let head = String::from_utf8_lossy(&request[..head_end]).to_ascii_lowercase();
    let body_len = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while request.len() < head_end + body_len {
        let read = socket.read(&mut chunk).await?;
        anyhow::ensure!(read > 0, "connection closed before the request body ended");
        request.extend_from_slice(&chunk[..read]);
    }
    Ok(())
}
