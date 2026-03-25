import asyncio
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from tools import query_tool, summarise_tool, compare_tool


def create_server() -> Server:
    app = Server("mcp-fi")

    # Collect tool definitions from each module
    all_tools: list[Tool] = [
        *query_tool.get_tools(),
        *summarise_tool.get_tools(),
        *compare_tool.get_tools(),
    ]

    # Single list_tools handler that returns all tools at once
    @app.list_tools()
    async def list_tools() -> list[Tool]:
        return all_tools

    # Single call_tool handler that routes to the right tool
    @app.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        if name in query_tool.TOOL_NAMES:
            return await query_tool.handle(name, arguments)
        if name in summarise_tool.TOOL_NAMES:
            return await summarise_tool.handle(name, arguments)
        if name in compare_tool.TOOL_NAMES:
            return await compare_tool.handle(name, arguments)
        return [TextContent(type="text", text=f"Error: unknown tool '{name}'")]

    return app


async def run():
    app = create_server()
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


if __name__ == "__main__":
    import sys
    try:
        asyncio.run(run())
    except Exception as e:
        print(f"Server startup error: {e}", file=sys.stderr)
        raise