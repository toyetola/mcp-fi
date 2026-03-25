import pytest
from unittest.mock import MagicMock, patch
from mcp.server import Server
from mcp.types import ListToolsRequest, CallToolRequest, CallToolRequestParams
import tools.compare_tool as compare_tool_module
from tools.compare_tool import _compare_results


# ─────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────

@pytest.fixture
def app():
    app = Server("test-server")

    @app.list_tools()
    async def list_tools():
        return compare_tool_module.get_tools()

    @app.call_tool()
    async def call_tool(name, arguments):
        return await compare_tool_module.handle(name, arguments)

    return app


@pytest.fixture
def mock_connector_a():
    connector = MagicMock()
    connector.query.return_value = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]
    return connector


@pytest.fixture
def mock_connector_b():
    connector = MagicMock()
    connector.query.return_value = [
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]
    return connector


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

async def call_list_tools(app):
    handler = app.request_handlers[ListToolsRequest]
    result = await handler(ListToolsRequest(method="tools/list"))
    return result.root.tools


async def call_tool(app, name, arguments):
    handler = app.request_handlers[CallToolRequest]
    result = await handler(CallToolRequest(
        method="tools/call",
        params=CallToolRequestParams(name=name, arguments=arguments)
    ))
    return result.root.content


def make_get_connector(connector_a, connector_b):
    """
    Returns a fake get_connector that returns connector_a for the first
    call and connector_b for the second — mimicking two different sources.
    """
    calls = []
    def _get_connector(source):
        calls.append(source)
        return connector_a if len(calls) == 1 else connector_b
    return _get_connector


# ─────────────────────────────────────────
# Tests
# ─────────────────────────────────────────

class TestCompareTool:

    # --- tool registration ---

    async def test_compare_data_tool_is_registered(self, app):
        """compare_data should appear in the list of available tools."""
        tools = await call_list_tools(app)
        tool_names = [t.name for t in tools]
        assert "compare_data" in tool_names

    async def test_tool_has_description(self, app):
        """compare_data tool should have a non-empty description."""
        tools = await call_list_tools(app)
        tool = next(t for t in tools if t.name == "compare_data")
        assert tool.description
        assert len(tool.description) > 0

    async def test_tool_schema_requires_source_a_source_b_and_sql(self, app):
        """Input schema should require source_a, source_b, and sql."""
        tools = await call_list_tools(app)
        tool = next(t for t in tools if t.name == "compare_data")
        assert "source_a" in tool.inputSchema["required"]
        assert "source_b" in tool.inputSchema["required"]
        assert "sql" in tool.inputSchema["required"]

    # --- comparison results ---

    async def test_returns_comparison_summary(self, app, mock_connector_a, mock_connector_b):
        """Should return a text summary of the comparison."""
        with patch("tools.compare_tool.get_connector",
                   side_effect=make_get_connector(mock_connector_a, mock_connector_b)):
            result = await call_tool(app, "compare_data", {
                "source_a": "source_a",
                "source_b": "source_b",
                "sql": "SELECT * FROM users"
            })
        assert len(result) == 1
        assert "source_a" in result[0].text.lower()
        assert "source_b" in result[0].text.lower()

    async def test_identifies_rows_only_in_source_a(self, app, mock_connector_a, mock_connector_b):
        """Alice is only in source_a — should appear in the diff."""
        with patch("tools.compare_tool.get_connector",
                   side_effect=make_get_connector(mock_connector_a, mock_connector_b)):
            result = await call_tool(app, "compare_data", {
                "source_a": "source_a",
                "source_b": "source_b",
                "sql": "SELECT * FROM users"
            })
        assert "Alice" in result[0].text

    async def test_identifies_rows_only_in_source_b(self, app, mock_connector_a, mock_connector_b):
        """Charlie is only in source_b — should appear in the diff."""
        with patch("tools.compare_tool.get_connector",
                   side_effect=make_get_connector(mock_connector_a, mock_connector_b)):
            result = await call_tool(app, "compare_data", {
                "source_a": "source_a",
                "source_b": "source_b",
                "sql": "SELECT * FROM users"
            })
        assert "Charlie" in result[0].text

    async def test_identical_sources_reports_no_differences(self, app, mock_connector_a):
        """When both sources return identical data, report they are identical."""
        with patch("tools.compare_tool.get_connector", return_value=mock_connector_a):
            result = await call_tool(app, "compare_data", {
                "source_a": "source_a",
                "source_b": "source_b",
                "sql": "SELECT * FROM users"
            })
        assert "identical" in result[0].text.lower()

    # --- both connectors are always closed ---

    async def test_both_connectors_always_closed(self, app, mock_connector_a, mock_connector_b):
        """Both connectors must be closed after comparison."""
        with patch("tools.compare_tool.get_connector",
                   side_effect=make_get_connector(mock_connector_a, mock_connector_b)):
            await call_tool(app, "compare_data", {
                "source_a": "source_a",
                "source_b": "source_b",
                "sql": "SELECT * FROM users"
            })
        mock_connector_a.close.assert_called_once()
        mock_connector_b.close.assert_called_once()

    async def test_connector_b_closed_even_if_query_a_fails(
        self, app, mock_connector_a, mock_connector_b
    ):
        """connector_b must still be closed even if source_a query fails."""
        mock_connector_a.query.side_effect = Exception("source_a failed")
        with patch("tools.compare_tool.get_connector",
                   side_effect=make_get_connector(mock_connector_a, mock_connector_b)):
            result = await call_tool(app, "compare_data", {
                "source_a": "source_a",
                "source_b": "source_b",
                "sql": "SELECT * FROM users"
            })
        mock_connector_b.close.assert_called_once()
        assert "error" in result[0].text.lower()

    # --- error handling ---

    async def test_missing_source_a_returns_error(self, app):
        result = await call_tool(app, "compare_data", {
            "source_b": "source_b",
            "sql": "SELECT * FROM users"
        })
        assert "error" in result[0].text.lower()

    async def test_missing_source_b_returns_error(self, app):
        result = await call_tool(app, "compare_data", {
            "source_a": "source_a",
            "sql": "SELECT * FROM users"
        })
        assert "error" in result[0].text.lower()

    async def test_missing_sql_returns_error(self, app):
        result = await call_tool(app, "compare_data", {
            "source_a": "source_a",
            "source_b": "source_b"
        })
        assert "error" in result[0].text.lower()

    async def test_invalid_source_returns_error(self, app):
        with patch("tools.compare_tool.get_connector",
                   side_effect=ValueError("Source 'bad' not found")):
            result = await call_tool(app, "compare_data", {
                "source_a": "bad",
                "source_b": "source_b",
                "sql": "SELECT * FROM users"
            })
        assert "error" in result[0].text.lower()


# ─────────────────────────────────────────
# _compare_results unit tests
# ─────────────────────────────────────────

class TestCompareResults:

    def test_identical_results_reports_identical(self):
        rows = [{"id": 1, "name": "Alice"}]
        result = _compare_results("a", rows, "b", rows)
        assert "identical" in result.lower()

    def test_detects_row_only_in_a(self):
        rows_a = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        rows_b = [{"id": 2, "name": "Bob"}]
        result = _compare_results("a", rows_a, "b", rows_b)
        assert "Alice" in result

    def test_detects_row_only_in_b(self):
        rows_a = [{"id": 1, "name": "Alice"}]
        rows_b = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        result = _compare_results("a", rows_a, "b", rows_b)
        assert "Bob" in result

    def test_counts_rows_in_both(self):
        rows_a = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        rows_b = [{"id": 2, "name": "Bob"}]
        result = _compare_results("a", rows_a, "b", rows_b)
        assert "1" in result

    def test_empty_both_sources(self):
        result = _compare_results("a", [], "b", [])
        assert "identical" in result.lower()

    def test_one_empty_source(self):
        rows_a = [{"id": 1, "name": "Alice"}]
        result = _compare_results("a", rows_a, "b", [])
        assert "Alice" in result