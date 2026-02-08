use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

async fn capture_single_request_auth_header() -> (
    String,
    oneshot::Receiver<Option<String>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let endpoint = format!("http://{addr}");

    let (tx, rx) = oneshot::channel::<Option<String>>();
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut buf = Vec::with_capacity(4096);
        loop {
            let mut tmp = [0_u8; 1024];
            let n = socket.read(&mut tmp).await.unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }

        let req = String::from_utf8_lossy(&buf);
        let auth = req.lines().find_map(|line| {
            let lower = line.to_ascii_lowercase();
            lower
                .strip_prefix("authorization:")
                .map(|_| line["authorization:".len()..].trim().to_string())
        });

        let _ = tx.send(auth);

        let response = concat!(
            "HTTP/1.1 200 OK\r\n",
            "Content-Length: 0\r\n",
            "ETag: \"etag\"\r\n",
            "Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n",
            "\r\n"
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });

    (endpoint, rx, handle)
}

async fn run_head_request(endpoint: &str, skip_signature: bool) {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer my-jwt-token"),
    );

    let client_options = ClientOptions::new()
        .with_allow_http(true)
        .with_default_headers(headers);

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name("test-bucket")
        .with_region("us-east-1")
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_client_options(client_options);

    if skip_signature {
        builder = builder.with_skip_signature(true);
    } else {
        builder = builder
            .with_access_key_id("access-key")
            .with_secret_access_key("secret-key");
    }

    let store = builder.build().unwrap();
    let _ = store.head(&Path::from("sample-object")).await.unwrap();
}

#[tokio::test]
async fn object_store_default_signing_replaces_custom_auth_header() {
    let (endpoint, rx, handle) = capture_single_request_auth_header().await;

    run_head_request(&endpoint, false).await;

    let auth = rx.await.unwrap().unwrap();
    assert!(
        auth.starts_with("AWS4-HMAC-SHA256 "),
        "expected SigV4 auth header, got: {auth}"
    );

    handle.await.unwrap();
}

#[tokio::test]
async fn object_store_skip_signature_preserves_custom_auth_header() {
    let (endpoint, rx, handle) = capture_single_request_auth_header().await;

    run_head_request(&endpoint, true).await;

    let auth = rx.await.unwrap().unwrap();
    assert_eq!(auth, "Bearer my-jwt-token");

    handle.await.unwrap();
}
