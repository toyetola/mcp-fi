import pytest
from mcp.types import ListToolsRequest
from server import create_server


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

async def get_registered_tools(app):
    handler = app.request_handlers[ListToolsRequest]
    result = await handler(ListToolsRequest(method="tools/list"))
    return result.root.tools


# ─────────────────────────────────────────
# Tests
# ─────────────────────────────────────────

class TestServer:

    def test_server_has_correct_name(self):
        """Server should be named mcp-fi."""
        app = create_server()
        assert app.name == "mcp-fi"

    async def test_all_three_tools_are_registered(self):
        """All three tools should be registered on startup."""
        app = create_server()
        tools = await get_registered_tools(app)
        tool_names = [t.name for t in tools]
        assert "query_data" in tool_names
        assert "summarise_data" in tool_names
        assert "compare_data" in tool_names

    async def test_server_has_exactly_three_tools(self):
        """Server should expose exactly three tools — no more, no less."""
        app = create_server()
        tools = await get_registered_tools(app)
        assert len(tools) == 3

    async def test_all_tools_have_descriptions(self):
        """Every registered tool must have a non-empty description."""
        app = create_server()
        tools = await get_registered_tools(app)
        for tool in tools:
            assert tool.description, f"Tool '{tool.name}' has no description"
            assert len(tool.description) > 0

    async def test_all_tools_have_input_schemas(self):
        """Every registered tool must have an input schema."""
        app = create_server()
        tools = await get_registered_tools(app)
        for tool in tools:
            assert tool.inputSchema, f"Tool '{tool.name}' has no inputSchema"