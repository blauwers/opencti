# coding: utf-8
"""OpenCTI MCP server entrypoint.

Starts the Model Context Protocol server and registers all tools and resources.
Supports both ``stdio`` (for direct LLM tool use) and ``sse`` (HTTP/SSE for
remote deployment) transports, controlled by the ``MCP_TRANSPORT`` environment
variable.

Usage::

    # stdio (default — for Claude Desktop / Copilot extension):
    OPENCTI_URL=http://localhost:4000 OPENCTI_TOKEN=<token> python -m opencti_mcp.server

    # SSE / HTTP (for remote deployment):
    MCP_TRANSPORT=sse MCP_SSE_HOST=0.0.0.0 MCP_SSE_PORT=8000 \\
        OPENCTI_URL=http://opencti:4000 OPENCTI_TOKEN=<token> \\
        python -m opencti_mcp.server

    # SSE with Bearer token authentication:
    MCP_TRANSPORT=sse MCP_API_KEY=<secret> ... python -m opencti_mcp.server
"""

from __future__ import annotations

import logging
import sys

from mcp.server.fastmcp import FastMCP

from opencti_mcp.client import init_client
from opencti_mcp.config import Config, load_config
from opencti_mcp.resources import stix_export
from opencti_mcp.tools import (
    cases,
    enrichment,
    indicators,
    investigations,
    observables,
    relationships,
    reports,
    search,
)

# ---------------------------------------------------------------------------
# Logging — write structured output to stderr so it does not pollute the
# MCP stdio protocol stream.
# ---------------------------------------------------------------------------
logging.basicConfig(
    stream=sys.stderr,
    format='{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":%(message)s}',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("opencti_mcp")


def _run_sse(mcp: FastMCP, cfg: Config) -> None:
    """Start SSE transport, optionally with Bearer token authentication.

    When ``MCP_API_KEY`` is set every incoming SSE request must carry an
    ``Authorization: Bearer <key>`` header; requests without it are rejected
    with HTTP 401.

    Falls back to the built-in ``mcp.run`` runner if ``mcp.sse_app()`` is not
    available (older mcp library versions), in which case auth is disabled.
    """
    try:
        app = mcp.sse_app()
    except AttributeError:
        logger.warning('"mcp.sse_app() not available — running without authentication middleware"')
        if cfg.api_key:
            logger.warning('"MCP_API_KEY is set but cannot be enforced without sse_app() support"')
        mcp.run(transport="sse")
        return

    if cfg.api_key:
        import uvicorn
        from starlette.middleware.base import BaseHTTPMiddleware
        from starlette.responses import Response

        _key = cfg.api_key

        class _BearerAuthMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request, call_next):  # type: ignore[override]
                auth = request.headers.get("Authorization", "")
                if not (auth.startswith("Bearer ") and auth[7:] == _key):
                    return Response(
                        content="Unauthorized",
                        status_code=401,
                        media_type="text/plain",
                    )
                return await call_next(request)

        app.add_middleware(_BearerAuthMiddleware)
        logger.info('"SSE transport: Bearer token authentication enabled"')
        uvicorn.run(app, host=cfg.sse_host, port=cfg.sse_port, log_level="warning")
    else:
        logger.warning('"SSE transport: MCP_API_KEY is not set — the endpoint is unauthenticated"')
        mcp.run(transport="sse")


def build_server() -> tuple[FastMCP, Config]:
    """Construct and configure the MCP server.

    Loads config, initialises the pycti client, and registers all tools and
    resources.  Returns the configured :class:`FastMCP` instance and the
    :class:`Config` so that the transport can be selected.

    :return: ``(mcp, cfg)`` tuple.
    :raises ValueError: if required environment variables are missing.
    """
    cfg = load_config()
    logger.setLevel(cfg.log_level.upper())
    logger.info('"Initialising OpenCTI MCP server"')

    init_client(cfg)

    mcp = FastMCP(
        "OpenCTI",
        instructions=(
            "You are connected to an OpenCTI threat-intelligence platform. "
            "Use the available tools to look up, create, and enrich indicators, "
            "observables, reports, cases (incident response, RFIs), and "
            "investigation workspaces.  All objects follow the STIX 2.1 standard."
        ),
    )

    # Register tools
    search.register(mcp)
    indicators.register(mcp)
    observables.register(mcp)
    reports.register(mcp)
    cases.register(mcp)
    investigations.register(mcp)
    enrichment.register(mcp)
    relationships.register(mcp)

    # Register resources (read-only context)
    stix_export.register(mcp)

    logger.info('"OpenCTI MCP server ready"')
    return mcp, cfg


def main() -> None:
    """Start the MCP server using the transport specified in the environment."""
    mcp, cfg = build_server()

    if cfg.transport == "sse":
        logger.info(f'"Starting SSE transport on {cfg.sse_host}:{cfg.sse_port}"')
        _run_sse(mcp, cfg)
    else:
        logger.info('"Starting stdio transport"')
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
