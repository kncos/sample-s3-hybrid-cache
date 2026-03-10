# Connection Pooling

## Overview

The proxy uses Hyper 1.0's built-in connection pooling to reuse TCP/TLS connections to S3. The `ConnectionPoolManager` handles DNS resolution and IP distribution; hyper manages actual TCP connection lifecycle.

### Architecture
- **Hyper Client**: Manages connection pooling, idle timeout, and reuse automatically
- **CustomHttpsConnector**: Establishes TCP+TLS connections, applies socket options (keepalive, receive buffer) via socket2, records failures in IpHealthTracker
- **ConnectionPoolManager**: DNS resolution, endpoint overrides, per-endpoint IpDistributor (round-robin), hostname lookup for TLS SNI
- **IpHealthTracker**: Lock-free per-IP failure tracking via DashMap. Excludes IPs after consecutive failures; DNS refresh restores them
- **RwLock**: Hot path (get_distributed_ip, get_hostname_for_ip) uses read locks. Write locks only for DNS refresh and IP exclusion

### Configuration

```yaml
connection_pool:
  keepalive_enabled: true        # Enable HTTP connection keepalive (default: true)
  idle_timeout: "55s"            # Just under S3's ~60s server-side timeout (default: 55s)
  max_idle_per_host: 100         # Max idle connections per host when IP distribution disabled (default: 100)
  max_idle_per_ip: 10            # Max idle connections per IP when IP distribution enabled (default: 10)
  dns_refresh_interval: "60s"    # DNS re-resolution interval (default: 60s)
  connection_timeout: "10s"      # TCP connect timeout (default: 10s)
  ip_distribution_enabled: true  # Per-IP connection pools via URI authority rewriting (default: true)
  keepalive_idle_secs: 15        # TCP_KEEPIDLE — seconds idle before first probe (default: 15)
  keepalive_interval_secs: 5     # TCP_KEEPINTVL — seconds between probes (default: 5)
  keepalive_retries: 3           # TCP_KEEPCNT — failed probes before dead (default: 3)
  tcp_recv_buffer_size: 262144   # SO_RCVBUF hint in bytes, null for kernel default (default: 256KB)
  ip_failure_threshold: 3        # Consecutive failures before IP exclusion (default: 3)
```

### Key Features

#### 1. Connection Reuse
- First request to an endpoint: full TCP + TLS handshake (~150-300ms)
- Subsequent requests: reuse pooled connection (~0ms overhead)
- Hyper automatically manages pool keyed by (scheme, authority, port)

#### 2. TCP Socket Options (socket2)
Applied after TCP connect, before TLS handshake on every new connection:
- **SO_KEEPALIVE**: Detects dead connections at the TCP layer before hyper tries to reuse them. Configured via `keepalive_idle_secs`, `keepalive_interval_secs`, `keepalive_retries`.
- **SO_RCVBUF**: 256KB receive buffer improves throughput for large object downloads. Configurable via `tcp_recv_buffer_size`.
- Failures log a warning and continue — socket options are best-effort.

#### 3. IP Health Tracking
- `IpHealthTracker` uses `DashMap<IpAddr, u32>` for lock-free concurrent access
- TCP connect failures, TLS handshake failures, and hyper connection errors increment the counter
- Successful responses reset the counter to zero
- When counter reaches `ip_failure_threshold` (default: 3), the IP is removed from the IpDistributor
- DNS refresh (every `dns_refresh_interval`) rebuilds the distributor with all resolved IPs, restoring excluded ones
- If all IPs are excluded, `get_distributed_ip` returns None and the request falls back to hostname-based routing

#### 4. Idle Timeout
- Default: 55 seconds (aligned with S3's ~60s server-side timeout)
- 5-second safety margin avoids reusing connections S3 is about to close
- Connections idle beyond this are automatically closed by Hyper

#### 5. Per-IP Pool Isolation
- When `ip_distribution_enabled: true`, the proxy rewrites URI authority from hostname to IP
- Hyper creates separate connection pools per IP address
- `max_idle_per_ip` controls idle connections per pool (default: 10)
- TLS SNI and Host headers use the original hostname for AWS SigV4 compatibility

#### 6. Error Recovery
- Connection errors automatically remove failed connections from hyper's pool
- Requests are retried with new connections (up to 3 retries for GET/HEAD)
- Connection errors don't count against retry limit
- IpHealthTracker excludes persistently failing IPs from round-robin

### Metrics
Connection metrics via `/metrics`:

```json
{
  "connection_pool": {
    "connections_created": { "s3.us-west-2.amazonaws.com": 15 },
    "connections_reused": { "s3.us-west-2.amazonaws.com": 142 },
    "idle_timeout_closures": 3,
    "error_closures": 1,
    "dns_refresh_count": 12,
    "ip_addresses": ["52.92.17.224", "52.92.17.225"]
  }
}
```

IP distribution stats via `/health`:

```json
{
  "ip_distribution": {
    "endpoints": [{
      "endpoint": "s3.us-west-2.amazonaws.com",
      "total_distributor_ips": 8,
      "ips": [{"ip": "52.92.17.224", "active_connections": 0, "idle_connections": 0}]
    }]
  }
}
```

### Troubleshooting

#### High Connection Creation Rate
If `connections_created` is high relative to `connections_reused`:
- Check if `idle_timeout` is too short (should be 55s for S3)
- Verify `keepalive_enabled` is true
- Check TCP keepalive settings — dead connections detected late cause pool misses

#### IPs Being Excluded
If IPs are being excluded from the distributor:
- Check application logs for "IP failure threshold reached" messages
- Verify network connectivity to S3 IPs
- Increase `ip_failure_threshold` if transient errors are common
- Reduce `dns_refresh_interval` for faster recovery of excluded IPs

#### Connection Errors
If `error_closures` is increasing:
- Check network stability and firewall/NAT timeout settings
- Verify S3 endpoint is healthy
- TCP keepalive should detect dead connections — check `keepalive_idle_secs` is not too high

### Disabling Connection Keepalive

```yaml
connection_pool:
  keepalive_enabled: false
```

This sets `pool_idle_timeout` and `pool_max_idle_per_host` to 0 in hyper, creating new connections for every request.

### Integration with Other Features
- **Load Balancing**: IpDistributor round-robin preserved across connection reuse
- **DNS Refresh**: Rebuilds distributor with all resolved IPs, restoring excluded ones
- **IP Health Tracking**: Failing IPs excluded from round-robin, auto-recovered on DNS refresh
- **Streaming**: Connections stay active during streaming, returned to pool after
- **Retry Logic**: Connection errors trigger retries without counting against limit
