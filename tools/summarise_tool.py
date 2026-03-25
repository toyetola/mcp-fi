from mcp.types import Tool, TextContent
from connectors.router import get_connector

TOOL_NAMES = {"summarise_data"}


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="summarise_data",
            description="""Summarise the structure of a named data source.
            Use this before querying to understand what tables exist and 
            what columns they contain. Returns table names, column names, 
            and column types. Always call this before query_data if you 
            are unfamiliar with the data source structure.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "description": "Name of the data source from config e.g. my_sqlite_db"
                    },
                    "table": {
                        "type": "string",
                        "description": "Optional. If provided, describes only that table. If omitted, describes all tables."
                    }
                },
                "required": ["source"]
            }
        )
    ]


async def handle(name: str, arguments: dict) -> list[TextContent]:
    source = arguments.get("source")
    table = arguments.get("table")

    if not source:
        return [TextContent(type="text", text="Error: 'source' argument is required")]

    connector = None
    try:
        connector = get_connector(source)

        if table:
            schema = connector.describe_table(table)
            if not schema:
                return [TextContent(
                    type="text",
                    text=f"Table '{table}' not found or has no columns."
                )]
            summary = _format_table_summary(table, schema)
            return [TextContent(type="text", text=summary)]

        tables = connector.list_tables()
        if not tables:
            return [TextContent(
                type="text",
                text=f"No tables found in source '{source}'."
            )]

        summaries = []
        for t in tables:
            schema = connector.describe_table(t)
            summaries.append(_format_table_summary(t, schema))

        return [TextContent(type="text", text="\n\n".join(summaries))]

    except ValueError as e:
        return [TextContent(type="text", text=f"Configuration error: {e}")]

    except Exception as e:
        return [TextContent(type="text", text=f"Summarise error: {e}")]

    finally:
        if connector:
            connector.close()


def _format_table_summary(table: str, schema: list[dict]) -> str:
    lines = [f"Table: {table}", "Columns:"]
    for col in schema:
        col_name = col.get("name") or col.get("column_name", "unknown")
        col_type = col.get("type") or col.get("data_type", "unknown")
        lines.append(f"  - {col_name} ({col_type})")
    return "\n".join(lines)