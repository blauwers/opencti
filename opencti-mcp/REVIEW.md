# MCP Server Code Review (Security + Production)

Date: 2026-05-03

## Scope reviewed

- `opencti-mcp/src/opencti_mcp/server.py`
- `opencti-mcp/src/opencti_mcp/config.py`
- `opencti-mcp/src/opencti_mcp/client.py`
- `opencti-mcp/src/opencti_mcp/tools/*.py`
- Deployment defaults in `opencti-mcp/Dockerfile` and `opencti-mcp/docker-compose.yml`

## Findings

### 1) **Critical**: SSE can start unauthenticated by default

- `MCP_API_KEY` is optional and missing key only logs a warning, then starts transport anyway.
- Docker defaults bind on `0.0.0.0:8000`, making accidental external exposure likely.

**Evidence**

- `server.py` starts unauthenticated SSE when `cfg.api_key` is empty.
- `Dockerfile` and `docker-compose.yml` default to `MCP_TRANSPORT=sse`, `MCP_SSE_HOST=0.0.0.0`.

**Impact**

Remote users can invoke high-privilege OpenCTI actions with the server's API token if the port is reachable.

**Recommendation**

Fail closed for SSE in production: require `MCP_API_KEY` (or stronger auth), and default bind host to `127.0.0.1` unless explicitly overridden.

---

### 2) **High**: Authentication fallback silently disables auth on older MCP libs

If `mcp.sse_app()` is unavailable, code falls back to `mcp.run(transport="sse")` and only logs warnings, even when `MCP_API_KEY` is set.

**Impact**

Deployment drift/version mismatch can silently remove authentication.

**Recommendation**

When `MCP_API_KEY` is set and `sse_app()` is missing, raise a hard startup error instead of running unauthenticated.

---

### 3) **High**: Error payloads expose internal details to MCP clients

Most tools catch broad exceptions and return `{"error": str(exc)}` directly.

**Impact**

Leaks backend internals (GraphQL details, stack-adjacent message content, object identifiers), aiding reconnaissance.

**Recommendation**

Return sanitized client-facing errors with stable error codes; log detailed exceptions server-side only.

---

### 4) **Medium**: No request rate/size controls for SSE endpoint

No middleware limits request body size, call rates, or concurrent in-flight operations.

**Impact**

DoS risk and uncontrolled load amplification against OpenCTI backend.

**Recommendation**

Add rate limiting, connection limits, and max payload controls at app or reverse proxy layer.

---

### 5) **Medium**: Logging format is pseudo-JSON and may break structured log pipelines

`logging.basicConfig` uses `"msg":%(message)s` without guaranteed JSON escaping.

**Impact**

Malformed log records and potential log injection/parse failures in SIEM pipelines.

**Recommendation**

Use a JSON formatter that escapes fields safely (e.g., python-json-logger or structlog).

---

### 6) **Medium**: Transport value and port parsing have weak validation

`transport` accepts arbitrary string (non-`sse` defaults implicitly to stdio path), and `int(MCP_SSE_PORT)` can throw uncaught `ValueError` with poor UX.

**Impact**

Misconfiguration can create surprising behavior and brittle startup.

**Recommendation**

Validate `transport in {"stdio", "sse"}` and port range `1..65535` with clear startup error messages.

---

### 7) **Low/Medium**: Global singleton client may limit future concurrency safety

`_client` is a module-global singleton.

**Impact**

Can complicate multi-tenant/server reuse scenarios and test isolation; future async/thread behaviors may become risky.

**Recommendation**

Keep for now if process-model is single-tenant, but document assumptions and prefer dependency injection for future evolution.

## Production readiness summary

- **Current posture**: Not safe-by-default for internet- or shared-network SSE deployment.
- **Minimum bar before production**:
  1. Require auth for SSE (hard fail if unavailable).
  2. Default to loopback bind and explicit opt-in for `0.0.0.0`.
  3. Sanitize user-visible errors.
  4. Add rate limiting / reverse proxy controls.
  5. Strengthen config validation and structured logging.

## Re-review update (post-hardening)

The following items are now **resolved** in code:

- SSE unauthenticated-by-default risk is mitigated by requiring explicit opt-in.
- Silent auth downgrade when `sse_app()` is unavailable (while API key is set) now fails hard.
- Transport and port validation have been added.

Remaining issues still worth addressing:

1. **High — raw exception leakage remains widespread**
   - Many tools/resources still return `{"error": str(exc)}` directly to clients.
   - This can expose backend internals and query/permission details.
   - Recommendation: centralize error mapping (`error_code`, safe `message`) and log full exception server-side only.

2. **Medium — no explicit rate limiting / concurrency controls at SSE layer**
   - The service still relies on upstream deployment controls.
   - Recommendation: add documented reverse-proxy controls (req/sec, burst, body size) and optional in-app limits.

3. **Medium — singleton client lifecycle assumptions are undocumented**
   - Current global client pattern is acceptable for single-process/single-tenant, but implicit.
   - Recommendation: document process model assumptions and migration path to DI for multi-tenant or highly concurrent modes.
