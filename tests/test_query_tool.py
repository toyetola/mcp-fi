import pytest
from unittest.mock import MagicMock, patch
from mcp.server import Server
from mcp.types import ListToolsRequest, CallToolRequest, CallToolRequestParams
import tools.query_tool as query_tool_module


# ─────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────

@pytest.fixture
def app():
    app = Server("test-server")

    @app.list_tools()
    async def list_tools():
        return query_tool_module.get_tools()

    @app.call_tool()
    async def call_tool(name, arguments):
        return await query_tool_module.handle(name, arguments)

    return app


@pytest.fixture
def mock_connector():
    """A fake connector that we can control in each test."""
    connector = MagicMock()
    connector.query.return_value = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
    return connector


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

async def call_list_tools(app):
    """Invoke the registered list_tools handler directly."""
    handler = app.request_handlers[ListToolsRequest]
    result = await handler(ListToolsRequest(method="tools/list"))
    return result.root.tools


async def call_tool(app, name, arguments):
    """Invoke the registered call_tool handler directly."""
    handler = app.request_handlers[CallToolRequest]
    result = await handler(CallToolRequest(
        method="tools/call",
        params=CallToolRequestParams(name=name, arguments=arguments)
    ))
    return result.root.content


# ─────────────────────────────────────────
# Tests
# ─────────────────────────────────────────

class TestQueryTool:

    # --- tool registration ---

    async def test_query_data_tool_is_registered(self, app):
        """query_data should appear in the list of available tools."""
        tools = await call_list_tools(app)
        tool_names = [t.name for t in tools]
        assert "query_data" in tool_names

    async def test_tool_has_description(self, app):
        """query_data tool should have a non-empty description."""
        tools = await call_list_tools(app)
        query_tool = next(t for t in tools if t.name == "query_data")
        assert query_tool.description
        assert len(query_tool.description) > 0

    async def test_tool_schema_requires_source_and_sql(self, app):
        """Input schema should require both source and sql."""
        tools = await call_list_tools(app)
        query_tool = next(t for t in tools if t.name == "query_data")
        assert "source" in query_tool.inputSchema["required"]
        assert "sql" in query_tool.inputSchema["required"]

    # --- successful queries ---

    async def test_returns_query_results(self, app, mock_connector):
        """Should return results from the connector as TextContent."""
        with patch("tools.query_tool.get_connector", return_value=mock_connector):
            result = await call_tool(app, "query_data", {
                "source": "my_sqlite_db",
                "sql": "SELECT * FROM users"
            })
        assert len(result) == 1
        assert "Alice" in result[0].text

    async def test_connector_is_always_closed(self, app, mock_connector):
        """close() must be called even on successful queries."""
        with patch("tools.query_tool.get_connector", return_value=mock_connector):
            await call_tool(app, "query_data", {
                "source": "my_sqlite_db",
                "sql": "SELECT * FROM users"
            })
        mock_connector.close.assert_called_once()

    async def test_empty_result_returns_message(self, app, mock_connector):
        """Empty query results should return a helpful message."""
        mock_connector.query.return_value = []
        with patch("tools.query_tool.get_connector", return_value=mock_connector):
            result = await call_tool(app, "query_data", {
                "source": "my_sqlite_db",
                "sql": "SELECT * FROM users WHERE name = 'Nobody'"
            })
        assert "no results" in result[0].text.lower()

    # --- error handling ---

    async def test_missing_source_returns_error(self, app):
        """Should return an error message if source argument is missing."""
        result = await call_tool(app, "query_data", {"sql": "SELECT 1"})
        assert "error" in result[0].text.lower()

    async def test_missing_sql_returns_error(self, app):
        """Should return an error message if sql argument is missing."""
        result = await call_tool(app, "query_data", {"source": "my_sqlite_db"})
        assert "error" in result[0].text.lower()

    async def test_invalid_source_returns_error(self, app):
        """Should return an error message if source is not in config."""
        with patch("tools.query_tool.get_connector",
                   side_effect=ValueError("Source 'bad' not found")):
            result = await call_tool(app, "query_data", {
                "source": "bad",
                "sql": "SELECT * FROM users"
            })
        assert "error" in result[0].text.lower()

    async def test_connector_closed_even_on_query_error(self, app, mock_connector):
        """close() must be called even when the query raises an exception."""
        mock_connector.query.side_effect = Exception("DB connection lost")
        with patch("tools.query_tool.get_connector", return_value=mock_connector):
            result = await call_tool(app, "query_data", {
                "source": "my_sqlite_db",
                "sql": "SELECT * FROM users"
            })
        mock_connector.close.assert_called_once()
        assert "error" in result[0].text.lower()