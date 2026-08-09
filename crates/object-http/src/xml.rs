//! S3-compatible XML response formatting.
//!
//! Produces the XML bodies required by S3 clients:
//! - [`list_bucket_result`] — `GET /bucket?prefix=…`
//! - [`error_response`] — 4xx/5xx error bodies

use chrono::{DateTime, Utc};
use plures_object_core::ObjectMeta;

/// Format an S3 `ListBucketResult` XML response.
pub fn list_bucket_result(
    bucket: &str,
    prefix: &str,
    max_keys: usize,
    is_truncated: bool,
    objects: &[ObjectMeta],
) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    out.push_str(&format!("  <Name>{}</Name>\n", xml_escape(bucket)));
    out.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(prefix)));
    out.push_str(&format!("  <MaxKeys>{max_keys}</MaxKeys>\n"));
    out.push_str(&format!("  <IsTruncated>{is_truncated}</IsTruncated>\n"));
    for obj in objects {
        out.push_str("  <Contents>\n");
        out.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&obj.key.0)));
        out.push_str(&format!("    <Size>{}</Size>\n", obj.size));
        out.push_str(&format!("    <ETag>\"{}\"</ETag>\n", xml_escape(&obj.etag)));
        out.push_str(&format!(
            "    <LastModified>{}</LastModified>\n",
            fmt_datetime(obj.updated_at)
        ));
        out.push_str("  </Contents>\n");
    }
    out.push_str("</ListBucketResult>");
    out
}

/// Format an S3 `Error` XML response.
pub fn error_response(code: &str, message: &str, resource: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <Error>\n\
           <Code>{}</Code>\n\
           <Message>{}</Message>\n\
           <Resource>{}</Resource>\n\
         </Error>",
        xml_escape(code),
        xml_escape(message),
        xml_escape(resource),
    )
}

/// Format an S3 `InitiateMultipartUploadResult` XML response.
pub fn initiate_multipart_upload_result(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
           <Bucket>{}</Bucket>\n\
           <Key>{}</Key>\n\
           <UploadId>{}</UploadId>\n\
         </InitiateMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id),
    )
}

/// Format an S3 `CompleteMultipartUploadResult` XML response.
pub fn complete_multipart_upload_result(bucket: &str, key: &str, etag: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
           <Bucket>{}</Bucket>\n\
           <Key>{}</Key>\n\
           <ETag>\"{}\"</ETag>\n\
         </CompleteMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(etag),
    )
}

/// Format an ISO 8601 datetime in the format S3 uses.
fn fmt_datetime(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// Parse a `CompleteMultipartUpload` XML request body into a list of
/// `(part_number, etag)` pairs.
///
/// Expected format:
/// ```xml
/// <CompleteMultipartUpload>
///   <Part>
///     <PartNumber>1</PartNumber>
///     <ETag>"abc123"</ETag>
///   </Part>
///   ...
/// </CompleteMultipartUpload>
/// ```
pub fn parse_complete_multipart_upload(body: &str) -> Result<Vec<(u32, String)>, String> {
    let mut parts = Vec::new();
    let mut rest = body;

    while let Some(start) = rest.find("<Part>") {
        let after_start = &rest[start + 6..];
        let end = after_start
            .find("</Part>")
            .ok_or("missing </Part>")?;
        let part_xml = &after_start[..end];
        rest = &after_start[end + 7..];

        let part_number = extract_tag_content(part_xml, "PartNumber")
            .ok_or("missing <PartNumber>")?
            .parse::<u32>()
            .map_err(|e| format!("invalid PartNumber: {e}"))?;

        let etag_raw = extract_tag_content(part_xml, "ETag")
            .ok_or("missing <ETag>")?;
        // Strip surrounding quotes if present (S3 convention).
        let etag = etag_raw.trim_matches('"').to_string();

        parts.push((part_number, etag));
    }

    if parts.is_empty() {
        return Err("no <Part> elements found".into());
    }

    Ok(parts)
}

/// Extract the text content of a simple XML tag like `<Tag>value</Tag>`.
fn extract_tag_content<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].trim())
}

/// Escape XML special characters.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_response_contains_code() {
        let xml = error_response("NoSuchKey", "The key does not exist.", "/bucket/key");
        assert!(xml.contains("<Code>NoSuchKey</Code>"));
        assert!(xml.contains("<Message>The key does not exist.</Message>"));
    }

    #[test]
    fn list_bucket_result_empty() {
        let xml = list_bucket_result("my-bucket", "", 1000, false, &[]);
        assert!(xml.contains("<Name>my-bucket</Name>"));
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<Contents>"));
    }

    #[test]
    fn xml_escape_special_chars() {
        let escaped = xml_escape("a&b<c>d\"e'f");
        assert_eq!(escaped, "a&amp;b&lt;c&gt;d&quot;e&apos;f");
    }
}
