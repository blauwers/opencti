# coding: utf-8
"""Tests for indicator tools."""

from __future__ import annotations

import json
from contextlib import AbstractContextManager
from unittest.mock import MagicMock, patch

import opencti_mcp.client as client_module
from opencti_mcp.tools import indicators

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_INDICATOR = {
    "id": "indicator--abc123",
    "standard_id": "indicator--abc123",
    "entity_type": "Indicator",
    "name": "[ipv4-addr:value = '1.2.3.4']",
    "pattern": "[ipv4-addr:value = '1.2.3.4']",
    "pattern_type": "stix",
    "x_opencti_score": 75,
    "valid_from": "2024-01-01T00:00:00Z",
    "valid_until": "2025-01-01T00:00:00Z",
}


def _mock_client() -> MagicMock:
    mock = MagicMock()
    mock.indicator.list.return_value = [MOCK_INDICATOR]
    mock.indicator.read.return_value = MOCK_INDICATOR
    mock.indicator.create.return_value = MOCK_INDICATOR
    mock.indicator.update_field.return_value = MOCK_INDICATOR
    mock.stix_cyber_observable.promote_to_indicator_v2.return_value = MOCK_INDICATOR
    mock.stix_core_relationship.list.return_value = []
    return mock


def _patch_client(mock: MagicMock) -> AbstractContextManager[None]:
    return patch.object(client_module, "_client", mock)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLookupIndicator:
    def test_returns_json_list(self) -> None:
        mock = _mock_client()
        with _patch_client(mock):
            from mcp.server.fastmcp import FastMCP

            mcp = FastMCP("test")
            indicators.register(mcp)
            # Access registered tools via mcp._tool_manager
            tool_fn = mcp._tool_manager._tools["lookup_indicator"].fn
            result = tool_fn(value="1.2.3.4")
            data = json.loads(result)
            assert isinstance(data, list)
            assert data[0]["id"] == "indicator--abc123"

    def test_clamps_limit(self) -> None:
        mock = _mock_client()
        with _patch_client(mock):
            from mcp.server.fastmcp import FastMCP

            mcp = FastMCP("test")
            indicators.register(mcp)
            tool_fn = mcp._tool_manager._tools["lookup_indicator"].fn
            tool_fn(value="test", limit=9999)
            mock.indicator.list.assert_called_once()
            _, kwargs = mock.indicator.list.call_args
            assert kwargs["first"] == 200

    def test_error_returns_json_error(self) -> None:
        mock = _mock_client()
        mock.indicator.list.side_effect = RuntimeError("connection failed")
        with _patch_client(mock):
            from mcp.server.fastmcp import FastMCP

            mcp = FastMCP("test")
            indicators.register(mcp)
            tool_fn = mcp._tool_manager._tools["lookup_indicator"].fn
            result = tool_fn(value="bad")
            data = json.loads(result)
            assert "error" in data


class TestGetIndicator:
    def test_returns_single_indicator(self) -> None:
        mock = _mock_client()
        with _patch_client(mock):
            from mcp.server.fastmcp import FastMCP

            mcp = FastMCP("test")
            indicators.register(mcp)
            tool_fn = mcp._tool_manager._tools["get_indicator"].fn
            result = tool_fn(indicator_id="indicator--abc123")
            data = json.loads(result)
            assert data["id"] == "indicator--abc123"

    def test_not_found(self) -> None:
        mock = _mock_client()
        mock.indicator.read.return_value = None
        with _patch_client(mock):
            from mcp.server.fastmcp import FastMCP

            mcp = FastMCP("test")
            indicators.register(mcp)
            tool_fn = mcp._tool_manager._tools["get_indicator"].fn
            result = tool_fn(indicator_id="missing")
            data = json.loads(result)
            assert "error" in data


class TestAddIndicator:
    def test_creates_indicator(self) -> None:
        mock = _mock_client()
        with _patch_client(mock):
            from mcp.server.fastmcp import FastMCP

            mcp = FastMCP("test")
            indicators.register(mcp)
            tool_fn = mcp._tool_manager._tools["add_indicator"].fn
            result = tool_fn(
                name="Test indicator",
                pattern="[ipv4-addr:value = '1.2.3.4']",
                pattern_type="stix",
                main_observable_type="IPv4-Addr",
                score=80,
            )
            data = json.loads(result)
            assert data["id"] == "indicator--abc123"
            mock.indicator.create.assert_called_once()


class TestUpdateIndicator:
    def test_updates_score(self) -> None:
        mock = _mock_client()
        with _patch_client(mock):
            from mcp.server.fastmcp import FastMCP

            mcp = FastMCP("test")
            indicators.register(mcp)
            tool_fn = mcp._tool_manager._tools["update_indicator"].fn
            result = tool_fn(indicator_id="indicator--abc123", score=90)
            data = json.loads(result)
            assert "error" not in data
            mock.indicator.update_field.assert_called_once()

    def test_no_fields_returns_error(self) -> None:
        mock = _mock_client()
        with _patch_client(mock):
            from mcp.server.fastmcp import FastMCP

            mcp = FastMCP("test")
            indicators.register(mcp)
            tool_fn = mcp._tool_manager._tools["update_indicator"].fn
            result = tool_fn(indicator_id="indicator--abc123")
            data = json.loads(result)
            assert "error" in data


class TestPromoteObservableToIndicator:
    def test_returns_indicator(self) -> None:
        mock = _mock_client()
        with _patch_client(mock):
            from mcp.server.fastmcp import FastMCP

            mcp = FastMCP("test")
            indicators.register(mcp)
            tool_fn = mcp._tool_manager._tools["promote_observable_to_indicator"].fn
            result = tool_fn(observable_id="ipv4-addr--xyz")
            data = json.loads(result)
            assert data["entity_type"] == "Indicator"
