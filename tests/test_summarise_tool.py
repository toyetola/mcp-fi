import pytest
from unittest.mock import MagicMock, patch
from mcp.server import Server
from mcp.types import ListToolsRequest, CallToolRequest, CallToolRequestParams
import tools.summarise_tool as summarise_tool_module
from tools.summarise_tool import _format_table_summary


# ─────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────

@pytest.fixture
def app():
    app = Server("test-server")

    @app.list_tools()
    async def list_tools():
        return summarise_tool_module.get_tools()

    @app.call_tool()
    async def call_tool(name, arguments):
        return await summarise_tool_module.handle(name, arguments)

    return app


@pytest.fixture
def mock_connector():
    connector = MagicMock()
    connector.list_tables.return_value = ["users", "products"]
    connector.describe_table.side_effect = lambda table: {
        "users": [
            {"name": "id",   "type": "INTEGER"},
            {"name": "name", "type": "TEXT"},
            {"name": "age",  "type": "INTEGER"},
        ],
        "products": [
            {"name": "id",    "type": "INTEGER"},
            {"name": "title", "type": "TEXT"},
            {"name": "price", "type": "REAL"},
        ]
    }.get(table, [])
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


# ─────────────────────────────────────────
# Tests
# ─────────────────────────────────────────

class TestSummariseTool:

    # --- tool registration ---

    async def test_summarise_data_tool_is_registered(self, app):
        """summarise_data should appear in the list of available tools."""
        tools = await call_list_tools(app)
        tool_names = [t.name for t in tools]
        assert "summarise_data" in tool_names

    async def test_tool_has_description(self, app):
        """summarise_data tool should have a non-empty description."""
        tools = await call_list_tools(app)
        tool = next(t for t in tools if t.name == "summarise_data")
        assert tool.description
        assert len(tool.description) > 0

    async def test_tool_schema_requires_only_source(self, app):
        """Only source should be required — table is optional."""
        tools = await call_list_tools(app)
        tool = next(t for t in tools if t.name == "summarise_data")
        assert "source" in tool.inputSchema["required"]
        assert "table" not in tool.inputSchema.get("required", [])

    # --- summarise all tables ---

    async def test_returns_all_tables_when_no_table_specified(self, app, mock_connector):
        """Without a table argument, all tables should be summarised."""
        with patch("tools.summarise_tool.get_connector", return_value=mock_connector):
            result = await call_tool(app, "summarise_data", {"source": "my_sqlite_db"})
        assert "users" in result[0].text
        assert "products" in result[0].text

    async def test_summary_includes_column_names(self, app, mock_connector):
        """Summary should include column names for each table."""
        with patch("tools.summarise_tool.get_connector", return_value=mock_connector):
            result = await call_tool(app, "summarise_data", {"source": "my_sqlite_db"})
        assert "name" in result[0].text
        assert "price" in result[0].text

    async def test_summary_includes_column_types(self, app, mock_connector):
        """Summary should include column types for each table."""
        with patch("tools.summarise_tool.get_connector", return_value=mock_connector):
            result = await call_tool(app, "summarise_data", {"source": "my_sqlite_db"})
        assert "INTEGER" in result[0].text
        assert "TEXT" in result[0].text

    async def test_no_tables_returns_message(self, app, mock_connector):
        """Empty source should return a helpful message."""
        mock_connector.list_tables.return_value = []
        with patch("tools.summarise_tool.get_connector", return_value=mock_connector):
            result = await call_tool(app, "summarise_data", {"source": "my_sqlite_db"})
        assert "no tables" in result[0].text.lower()

    # --- summarise single table ---

    async def test_returns_only_requested_table(self, app, mock_connector):
        """With a table argument, only that table should be summarised."""
        with patch("tools.summarise_tool.get_connector", return_value=mock_connector):
            result = await call_tool(app, "summarise_data", {
                "source": "my_sqlite_db",
                "table": "users"
            })
        assert "users" in result[0].text
        assert "products" not in result[0].text

    async def test_unknown_table_returns_message(self, app, mock_connector):
        """Requesting a non-existent table should return a helpful message."""
        mock_connector.describe_table.return_value = []
        with patch("tools.summarise_tool.get_connector", return_value=mock_connector):
            result = await call_tool(app, "summarise_data", {
                "source": "my_sqlite_db",
                "table": "nonexistent"
            })
        assert "not found" in result[0].text.lower()

    # --- error handling ---

    async def test_missing_source_returns_error(self, app):
        """Should return an error message if source argument is missing."""
        result = await call_tool(app, "summarise_data", {})
        assert "error" in result[0].text.lower()

    async def test_invalid_source_returns_error(self, app):
        """Should return an error if source is not in config."""
        with patch("tools.summarise_tool.get_connector",
                   side_effect=ValueError("Source 'bad' not found")):
            result = await call_tool(app, "summarise_data", {"source": "bad"})
        assert "error" in result[0].text.lower()

    async def test_connector_always_closed(self, app, mock_connector):
        """close() must always be called even on failure."""
        with patch("tools.summarise_tool.get_connector", return_value=mock_connector):
            await call_tool(app, "summarise_data", {"source": "my_sqlite_db"})
        mock_connector.close.assert_called_once()


# ─────────────────────────────────────────
# _format_table_summary unit tests
# ─────────────────────────────────────────

class TestFormatTableSummary:

    def test_includes_table_name(self):
        schema = [{"name": "id", "type": "INTEGER"}]
        result = _format_table_summary("users", schema)
        assert "users" in result

    def test_includes_column_name(self):
        schema = [{"name": "email", "type": "TEXT"}]
        result = _format_table_summary("users", schema)
        assert "email" in result

    def test_includes_column_type(self):
        schema = [{"name": "age", "type": "INTEGER"}]
        result = _format_table_summary("users", schema)
        assert "INTEGER" in result

    def test_handles_sqlite_format(self):
        """SQLite uses 'name' and 'type' keys."""
        schema = [{"name": "id", "type": "INTEGER"}]
        result = _format_table_summary("users", schema)
        assert "id" in result
        assert "INTEGER" in result

    def test_handles_postgres_duckdb_format(self):
        """Postgres and DuckDB use 'column_name' and 'data_type' keys."""
        schema = [{"column_name": "id", "data_type": "integer"}]
        result = _format_table_summary("users", schema)
        assert "id" in result
        assert "integer" in result

    def test_multiple_columns(self):
        schema = [
            {"name": "id",   "type": "INTEGER"},
            {"name": "name", "type": "TEXT"},
            {"name": "age",  "type": "INTEGER"},
        ]
        result = _format_table_summary("users", schema)
        assert "id" in result
        assert "name" in result
        assert "age" in result