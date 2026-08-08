//! Integration tests for the S3-compatible HTTP API.
//!
//! Each test spins up a real axum server on an ephemeral port and exercises
//! the API via `reqwest`.

use std::sync::Arc;

use plures_chunkstore::MemChunkStore;
use plures_manifest::MemManifestStore;
use plures_object_http::make_router;
use plures_object_store::ObjectService;
use reqwest::{Client, StatusCode};

// ── Test harness ─────────────────────────────────────────────────────────────

/// Start a server on a random port and return the base URL + client.
async fn start_server() -> (String, Client) {
    let service = Arc::new(
        ObjectService::new(Arc::new(MemChunkStore::new()), Arc::new(MemManifestStore::new()))
            .with_chunk_size(4), // Small chunks so multi-chunk paths are exercised
    );

    let app = make_router(service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = Client::builder()
        .build()
        .expect("reqwest client");

    (base_url, client)
}

// ── PUT ───────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn put_object_returns_200_with_etag() {
    let (base, client) = start_server().await;

    let resp = client
        .put(format!("{base}/test-bucket/hello.txt"))
        .header("Content-Type", "text/plain")
        .body("hello, world!")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let etag = resp.headers().get("etag").expect("ETag header");
    assert!(etag.to_str().unwrap().starts_with('"'));
}

#[tokio::test]
async fn put_object_overwrites() {
    let (base, client) = start_server().await;
    let url = format!("{base}/bucket/overwrite-me");

    client.put(&url).body("v1").send().await.unwrap();
    client.put(&url).body("v2").send().await.unwrap();

    let resp = client.get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();
    assert_eq!(body, "v2");
}

// ── GET ───────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn get_object_returns_body_and_headers() {
    let (base, client) = start_server().await;
    let url = format!("{base}/my-bucket/greet.txt");

    client
        .put(&url)
        .header("Content-Type", "text/plain; charset=utf-8")
        .body("Hello!")
        .send()
        .await
        .unwrap();

    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().contains_key("etag"));
    assert!(resp.headers().contains_key("content-length"));
    assert!(resp.headers().contains_key("last-modified"));

    let body = resp.text().await.unwrap();
    assert_eq!(body, "Hello!");
}

#[tokio::test]
async fn get_object_large_streaming() {
    let (base, client) = start_server().await;
    let url = format!("{base}/bucket/large");

    // 1 KiB — spans many 4-byte chunks (server chunk_size = 4)
    let payload: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
    client
        .put(&url)
        .body(payload.clone())
        .send()
        .await
        .unwrap();

    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        "1024"
    );

    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), payload.as_slice());
}

#[tokio::test]
async fn get_object_not_found_returns_404() {
    let (base, client) = start_server().await;

    let resp = client
        .get(format!("{base}/bucket/no-such-key"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<Code>NoSuchKey</Code>"));
}

// ── DELETE ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn delete_object_returns_204() {
    let (base, client) = start_server().await;
    let url = format!("{base}/bucket/deletable");

    client.put(&url).body("data").send().await.unwrap();

    let resp = client.delete(&url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Object should now be gone
    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_nonexistent_returns_404() {
    let (base, client) = start_server().await;

    let resp = client
        .delete(format!("{base}/bucket/ghost"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ── HEAD ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn head_object_returns_headers_no_body() {
    let (base, client) = start_server().await;
    let url = format!("{base}/bucket/headme");

    client
        .put(&url)
        .header("Content-Type", "image/png")
        .body(vec![0u8; 64])
        .send()
        .await
        .unwrap();

    let resp = client.head(&url).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().contains_key("etag"));
    assert_eq!(
        resp.headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        "64"
    );
    // HEAD must have empty body
    let body = resp.bytes().await.unwrap();
    assert!(body.is_empty());
}

#[tokio::test]
async fn head_nonexistent_returns_404() {
    let (base, client) = start_server().await;

    let resp = client
        .head(format!("{base}/bucket/nosuchkey"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ── LIST ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn list_objects_returns_xml() {
    let (base, client) = start_server().await;
    let bucket = "list-bucket";

    client
        .put(format!("{base}/{bucket}/a.txt"))
        .body("a")
        .send()
        .await
        .unwrap();
    client
        .put(format!("{base}/{bucket}/b.txt"))
        .body("b")
        .send()
        .await
        .unwrap();
    client
        .put(format!("{base}/{bucket}/sub/c.txt"))
        .body("c")
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base}/{bucket}"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<ListBucketResult"));
    assert!(body.contains("<Name>list-bucket</Name>"));
    assert!(body.contains("<Key>a.txt</Key>"));
    assert!(body.contains("<Key>b.txt</Key>"));
    assert!(body.contains("<Key>sub/c.txt</Key>"));
}

#[tokio::test]
async fn list_objects_with_prefix() {
    let (base, client) = start_server().await;
    let bucket = "prefix-bucket";

    client
        .put(format!("{base}/{bucket}/photos/a.jpg"))
        .body("img")
        .send()
        .await
        .unwrap();
    client
        .put(format!("{base}/{bucket}/photos/b.jpg"))
        .body("img")
        .send()
        .await
        .unwrap();
    client
        .put(format!("{base}/{bucket}/docs/c.pdf"))
        .body("pdf")
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base}/{bucket}?prefix=photos/"))
        .send()
        .await
        .unwrap();

    let body = resp.text().await.unwrap();
    assert!(body.contains("<Key>photos/a.jpg</Key>"));
    assert!(body.contains("<Key>photos/b.jpg</Key>"));
    assert!(!body.contains("<Key>docs/c.pdf</Key>"));
}

#[tokio::test]
async fn list_objects_max_keys() {
    let (base, client) = start_server().await;
    let bucket = "maxkeys-bucket";

    for i in 0..5 {
        client
            .put(format!("{base}/{bucket}/key-{i}"))
            .body("x")
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .get(format!("{base}/{bucket}?max-keys=2"))
        .send()
        .await
        .unwrap();

    let body = resp.text().await.unwrap();
    // max_keys=2, so only 2 <Contents> entries
    let count = body.matches("<Contents>").count();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn list_empty_bucket_returns_empty_xml() {
    let (base, client) = start_server().await;

    let resp = client
        .get(format!("{base}/empty-bucket"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<ListBucketResult"));
    assert!(!body.contains("<Contents>"));
}

// ── MULTIPART UPLOAD ─────────────────────────────────────────────────────────

#[tokio::test]
async fn multipart_initiate_returns_upload_id() {
    let (base, client) = start_server().await;

    let resp = client
        .post(format!("{base}/bucket/multi/key.bin?uploads"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<InitiateMultipartUploadResult"));
    assert!(body.contains("<Bucket>bucket</Bucket>"));
    assert!(body.contains("<Key>multi/key.bin</Key>"));
    assert!(body.contains("<UploadId>"));
}

/// Helper: extract the UploadId from an InitiateMultipartUploadResult XML.
fn extract_upload_id(xml: &str) -> String {
    let start = xml.find("<UploadId>").unwrap() + "<UploadId>".len();
    let end = xml[start..].find("</UploadId>").unwrap() + start;
    xml[start..end].to_string()
}

/// Helper: extract the ETag header value (without quotes).
fn extract_etag(resp: &reqwest::Response) -> String {
    resp.headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .trim_matches('"')
        .to_string()
}

#[tokio::test]
async fn multipart_upload_part_returns_etag() {
    let (base, client) = start_server().await;

    // Initiate
    let resp = client
        .post(format!("{base}/bucket/mp/obj?uploads"))
        .send()
        .await
        .unwrap();
    let upload_id = extract_upload_id(&resp.text().await.unwrap());

    // Upload part 1
    let resp = client
        .put(format!(
            "{base}/bucket/mp/obj?partNumber=1&uploadId={upload_id}"
        ))
        .body("part one")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().contains_key("etag"));
}

#[tokio::test]
async fn multipart_complete_assembles_object() {
    let (base, client) = start_server().await;

    // Initiate
    let resp = client
        .post(format!("{base}/bucket/mp/full?uploads"))
        .send()
        .await
        .unwrap();
    let upload_id = extract_upload_id(&resp.text().await.unwrap());

    // Upload part 1
    let resp = client
        .put(format!(
            "{base}/bucket/mp/full?partNumber=1&uploadId={upload_id}"
        ))
        .body("hello ")
        .send()
        .await
        .unwrap();
    let etag1 = extract_etag(&resp);

    // Upload part 2
    let resp = client
        .put(format!(
            "{base}/bucket/mp/full?partNumber=2&uploadId={upload_id}"
        ))
        .body("world")
        .send()
        .await
        .unwrap();
    let etag2 = extract_etag(&resp);

    // Complete
    let complete_body = format!(
        "<CompleteMultipartUpload>\
           <Part><PartNumber>1</PartNumber><ETag>\"{etag1}\"</ETag></Part>\
           <Part><PartNumber>2</PartNumber><ETag>\"{etag2}\"</ETag></Part>\
         </CompleteMultipartUpload>"
    );
    let resp = client
        .post(format!(
            "{base}/bucket/mp/full?uploadId={upload_id}"
        ))
        .body(complete_body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<CompleteMultipartUploadResult"));
    assert!(body.contains("<ETag>"));

    // Verify the assembled object is retrievable.
    let resp = client
        .get(format!("{base}/bucket/mp/full"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.bytes().await.unwrap();
    assert_eq!(&*body, b"hello world");
}

#[tokio::test]
async fn multipart_abort_cleans_up() {
    let (base, client) = start_server().await;

    // Initiate
    let resp = client
        .post(format!("{base}/bucket/mp/abort?uploads"))
        .send()
        .await
        .unwrap();
    let upload_id = extract_upload_id(&resp.text().await.unwrap());

    // Upload a part
    client
        .put(format!(
            "{base}/bucket/mp/abort?partNumber=1&uploadId={upload_id}"
        ))
        .body("partial")
        .send()
        .await
        .unwrap();

    // Abort
    let resp = client
        .delete(format!(
            "{base}/bucket/mp/abort?uploadId={upload_id}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Object should not exist
    let resp = client
        .get(format!("{base}/bucket/mp/abort"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn multipart_abort_unknown_returns_404() {
    let (base, client) = start_server().await;

    let resp = client
        .delete(format!("{base}/bucket/mp/x?uploadId=nonexistent"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn put_object_still_works_without_multipart_params() {
    let (base, client) = start_server().await;
    let url = format!("{base}/bucket/normal-put");

    let resp = client.put(&url).body("normal data").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client.get(&url).send().await.unwrap();
    assert_eq!(resp.text().await.unwrap(), "normal data");
}
