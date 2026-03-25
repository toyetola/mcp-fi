from mcp.types import Tool, TextContent
from connectors.router import get_connector

TOOL_NAMES = {"query_data"}


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="query_data",
            description="""Run a SQL query against a named data source.
            Use this when you need to fetch, filter, or aggregate data.
            Always call list_data_sources first if you don't know what 
            sources are available, and summarise_data first if you don't 
            know the table structure.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "description": "Name of the data source from config e.g. my_sqlite_db"
                    },
                    "sql": {
                        "type": "string",
                        "description": "Valid SQL query to execute against the source"
                    }
                },
                "required": ["source", "sql"]
            }
        )
    ]


async def handle(name: str, arguments: dict) -> list[TextContent]:
    source = arguments.get("source")
    sql = arguments.get("sql")

    if not source:
        return [TextContent(type="text", text="Error: 'source' argument is required")]
    if not sql:
        return [TextContent(type="text", text="Error: 'sql' argument is required")]

    connector = None
    try:
        connector = get_connector(source)
        results = connector.query(sql)

        if not results:
            return [TextContent(type="text", text="Query returned no results.")]

        return [TextContent(type="text", text=str(results))]

    except ValueError as e:
        return [TextContent(type="text", text=f"Configuration error: {e}")]

    except Exception as e:
        return [TextContent(type="text", text=f"Query error: {e}")]

    finally:
        if connector:
            connector.close()