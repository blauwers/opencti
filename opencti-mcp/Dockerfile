# syntax=docker/dockerfile:1
# Build the OpenCTI MCP server image.
#
# Usage:
#   docker build -t opencti-mcp .
#   docker run --rm \
#     -e OPENCTI_URL=http://opencti:4000 \
#     -e OPENCTI_TOKEN=<token> \
#     -e MCP_TRANSPORT=sse \
#     -e MCP_SSE_HOST=0.0.0.0 \
#     -e MCP_SSE_PORT=8000 \
#     -p 8000:8000 \
#     opencti-mcp

FROM python:3.12-slim

WORKDIR /app

# Copy package metadata first for better layer caching
COPY pyproject.toml ./
COPY src/ ./src/

# Install the package (no dev extras needed at runtime)
RUN pip install --no-cache-dir .

# Run as non-root for security
RUN useradd --no-create-home --shell /bin/false mcp
USER mcp

ENV MCP_TRANSPORT=sse \
    MCP_SSE_HOST=0.0.0.0 \
    MCP_SSE_PORT=8000

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/sse')" || exit 1

CMD ["opencti-mcp"]
