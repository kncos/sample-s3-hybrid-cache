//! Integration test: `build_s3_request_context` must produce a valid URI when given
//! an IPv6 host string (as extracted by `parse_host_header` from a bracketed
//! `Host` header like `[::1]:8081`).
//!
//! Spinning up the full HTTP listener and routing to an upstream would be invasive.
//! This test drives the boundary that matters end-to-end: after the proxy extracts
//! the unbracketed host from the `Host` header, the request-building code path
//! must re-bracket the IPv6 literal so hyper accepts the URI. If that round-trip
//! works, an IPv6 `Host` header is no longer rejected for Host parsing reasons
//! (it may still 502 on an unreachable upstream — that's acceptable).
//!
//! Validates: Requirements 6.1, 6.2

use hyper::{Method, Uri};
use s3_proxy::s3_client::build_s3_request_context;
use std::collections::HashMap;

#[test]
fn test_build_s3_request_context_accepts_ipv6_host() {
    // Validates: Requirements 6.1, 6.2
    //
    // After `parse_host_header` strips brackets from `"[::1]:8081"` it returns
    // `"::1"`. `build_s3_request_context` must re-bracket the IPv6 literal when
    // composing the absolute URI so hyper parses it.
    let method = Method::GET;
    let uri: Uri = "/bucket/key".parse().unwrap(); // relative — triggers absolute-URI composition
    let headers = HashMap::new();
    let host = "::1".to_string(); // unbracketed IPv6, as returned by parse_host_header

    let ctx = build_s3_request_context(method, uri, headers, None, host);

    let uri_str = ctx.uri.to_string();
    assert!(
        uri_str.starts_with("https://[::1]"),
        "Expected IPv6 authority to be bracketed in URI, got: {}",
        uri_str
    );
    assert!(
        uri_str.contains("/bucket/key"),
        "Expected path to survive URI composition, got: {}",
        uri_str
    );
}

#[test]
fn test_build_s3_request_context_accepts_long_ipv6_host() {
    // Validates: Requirements 6.1, 6.2
    //
    // Same defence for a full-length IPv6 literal (as would appear after parsing
    // `Host: [2001:db8::1]:443`).
    let method = Method::GET;
    let uri: Uri = "/bucket/key".parse().unwrap();
    let headers = HashMap::new();
    let host = "2001:db8::1".to_string();

    let ctx = build_s3_request_context(method, uri, headers, None, host);

    let uri_str = ctx.uri.to_string();
    assert!(
        uri_str.starts_with("https://[2001:db8::1]"),
        "Expected IPv6 authority to be bracketed in URI, got: {}",
        uri_str
    );
}

#[test]
fn test_build_s3_request_context_plain_hostname_unchanged() {
    // Validates: Requirement 6.10 (no regression for well-formed non-IPv6 hosts)
    let method = Method::GET;
    let uri: Uri = "/bucket/key".parse().unwrap();
    let headers = HashMap::new();
    let host = "example.com".to_string();

    let ctx = build_s3_request_context(method, uri, headers, None, host);

    let uri_str = ctx.uri.to_string();
    assert!(
        uri_str.starts_with("https://example.com/"),
        "Expected plain hostname to be preserved unchanged, got: {}",
        uri_str
    );
}
