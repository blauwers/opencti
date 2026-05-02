# OpenCTI MCP Server

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) server that exposes [OpenCTI](https://opencti.io/) threat-intelligence capabilities to AI assistants such as Claude and GitHub Copilot.

The server bridges MCP tool calls to the OpenCTI GraphQL API via [pycti](https://github.com/OpenCTI-Platform/client-python), the official Python client library.

---

## Features

| Category | Tools |
|----------|-------|
| **Indicators** | `lookup_indicator`, `list_indicators`, `get_indicator`, `add_indicator`, `update_indicator`, `promote_observable_to_indicator`, `get_indicator_relationships` |
| **Observables** | `lookup_observable`, `list_observables`, `get_observable`, `add_observable`, `enrich_observable`, `get_observable_indicators`, `get_observable_relationships` |
| **Reports** | `lookup_report`, `list_reports`, `create_report`, `add_object_to_report`, `get_report_objects`, `export_report_stix` |
| **Cases** | `create_incident_case`, `create_rfi`, `lookup_case`, `list_cases`, `add_object_to_case`, `update_case_status` |
| **Tasks** | `create_task`, `complete_task` |
| **Investigations** | `create_investigation`, `get_investigation`, `list_investigations`, `add_to_investigation`, `export_investigation_as_report`, `start_investigation_from_container` |
| **Enrichment** | `list_enrichment_connectors`, `enrich_entity`, `get_enrichment_status`, `get_entity_connectors` |
| **Relationships** | `create_relationship`, `lookup_relationships`, `create_sighting` |
| **Search** | `global_search`, `find_by_stix_id`, `find_by_external_reference` |

### MCP Resources (read-only context)

| URI template | Description |
|---|---|
| `opencti://indicator/{id}` | Full indicator details |
| `opencti://observable/{id}` | Observable with related indicators |
| `opencti://report/{id}` | Report with all contained objects |
| `opencti://case/{id}` | Case (incident/RFI/RFT) details |
| `opencti://investigation/{id}` | Investigation as a STIX 2.1 bundle |

---

## Requirements

- Python 3.10+
- A running OpenCTI instance (≥ 6.0)
- An OpenCTI API token with appropriate permissions

---

## Installation

```bash
# From the repository root
pip install -e ./opencti-mcp

# Or install directly once published
pip install opencti-mcp
```

---

## Configuration

All settings are controlled via environment variables (or a `.env` file in the working directory):

| Variable | Required | Default | Description |
|---|---|---|---|
| `OPENCTI_URL` | ✅ | — | Base URL of the OpenCTI instance (e.g. `http://localhost:4000`) |
| `OPENCTI_TOKEN` | ✅ | — | API bearer token |
| `OPENCTI_SSL_VERIFY` | | `true` | `true`/`false` or path to a CA bundle file |
| `LOG_LEVEL` | | `info` | Python log level (`debug`, `info`, `warning`, `error`) |
| `MAX_RESULTS` | | `100` | Default maximum entities returned per list call |
| `MCP_TRANSPORT` | | `stdio` | `stdio` or `sse` |
| `MCP_SSE_HOST` | | `127.0.0.1` | Bind host for SSE transport |
| `MCP_SSE_PORT` | | `8000` | Bind port for SSE transport |

---

## Usage

### stdio transport (Claude Desktop / Copilot extension)

Add to `~/.config/claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "opencti": {
      "command": "opencti-mcp",
      "env": {
        "OPENCTI_URL": "http://localhost:4000",
        "OPENCTI_TOKEN": "your-api-token-here"
      }
    }
  }
}
```

Or run directly:

```bash
OPENCTI_URL=http://localhost:4000 OPENCTI_TOKEN=<token> opencti-mcp
```

### SSE / HTTP transport (remote deployment)

```bash
MCP_TRANSPORT=sse \
MCP_SSE_HOST=0.0.0.0 \
MCP_SSE_PORT=8000 \
OPENCTI_URL=http://opencti:4000 \
OPENCTI_TOKEN=<token> \
opencti-mcp
```

### Docker

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install -e .
ENV MCP_TRANSPORT=sse MCP_SSE_HOST=0.0.0.0 MCP_SSE_PORT=8000
EXPOSE 8000
CMD ["opencti-mcp"]
```

```bash
docker run -e OPENCTI_URL=http://opencti:4000 -e OPENCTI_TOKEN=<token> -p 8000:8000 opencti-mcp
```

---

## Example Prompts

Once connected to Claude or GitHub Copilot:

```
Look up any indicators related to the IP address 185.220.101.45
```

```
Create an incident response case called "APT41 Intrusion - June 2024"
with high severity and P2 priority, then add the indicator
indicator--abc123 to it.
```

```
Find all reports published in the last 30 days that mention "Cobalt Strike"
and export the most recent one as STIX.
```

```
Create an investigation from the report report--xyz and export it as a
STIX bundle.
```

```
Trigger VirusTotal enrichment for the observable domain-name--abc and
poll until it completes.
```

---

## Development

```bash
cd opencti-mcp

# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Lint
black src/ tests/
isort src/ tests/
flake8 src/ tests/

# Type check
mypy src/
```

---

## Security Considerations

- **Token protection**: the API token is read from an environment variable and is never written to disk or logged.
- **Least privilege**: create a dedicated OpenCTI API token with only the permissions required (typically `KNOWLEDGE_READ`, `KNOWLEDGE_KNUPDATE`, `EXPLORE`).
- **Network exposure**: when using `sse` transport, bind to `127.0.0.1` unless you have a reverse proxy with authentication in front.
- **TLS**: set `OPENCTI_SSL_VERIFY=true` (default) in production.  Provide a CA bundle path when using a self-signed certificate.
- **Rate limiting**: all list tools cap results at 200 per call to avoid overwhelming the API.  Adjust `MAX_RESULTS` if needed.

---

## Architecture

```
MCP host (Claude / Copilot)
        │  MCP protocol (stdio or SSE)
        ▼
  opencti_mcp.server  (FastMCP)
        │
  tools/*.py          ── thin wrappers, delegate to pycti
  resources/*.py      ── read-only STIX context
        │
  opencti_mcp.client  ── singleton OpenCTIApiClient
        │  GraphQL / HTTP
        ▼
  OpenCTI platform
```

All tool modules are "thin" — they do the minimum input validation required and delegate all API logic to pycti.  This ensures that OpenCTI's STIX ID generation, canonicalisation, enrichment dispatch, and access-control enforcement remain in pycti where they belong.
