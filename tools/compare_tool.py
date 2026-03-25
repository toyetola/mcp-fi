from mcp.types import Tool, TextContent
from connectors.router import get_connector

TOOL_NAMES = {"compare_data"}


def get_tools() -> list[Tool]:
    return [
        Tool(
            name="compare_data",
            description="""Compare the results of the same SQL query across 
            two different data sources. Use this to find differences between 
            databases, validate data migrations, or check consistency between 
            a CSV export and a live database. Returns rows only in source_a, 
            rows only in source_b, and rows in both.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "source_a": {
                        "type": "string",
                        "description": "Name of the first data source from config"
                    },
                    "source_b": {
                        "type": "string",
                        "description": "Name of the second data source from config"
                    },
                    "sql": {
                        "type": "string",
                        "description": "SQL query to run against both sources"
                    }
                },
                "required": ["source_a", "source_b", "sql"]
            }
        )
    ]


async def handle(name: str, arguments: dict) -> list[TextContent]:
    source_a = arguments.get("source_a")
    source_b = arguments.get("source_b")
    sql = arguments.get("sql")

    if not source_a:
        return [TextContent(type="text", text="Error: 'source_a' argument is required")]
    if not source_b:
        return [TextContent(type="text", text="Error: 'source_b' argument is required")]
    if not sql:
        return [TextContent(type="text", text="Error: 'sql' argument is required")]

    connector_a = None
    connector_b = None
    try:
        connector_a = get_connector(source_a)
        connector_b = get_connector(source_b)

        results_a = connector_a.query(sql)
        results_b = connector_b.query(sql)

        comparison = _compare_results(source_a, results_a, source_b, results_b)
        return [TextContent(type="text", text=comparison)]

    except ValueError as e:
        return [TextContent(type="text", text=f"Configuration error: {e}")]

    except Exception as e:
        return [TextContent(type="text", text=f"Compare error: {e}")]

    finally:
        if connector_a:
            connector_a.close()
        if connector_b:
            connector_b.close()


def _compare_results(
    source_a: str,
    results_a: list[dict],
    source_b: str,
    results_b: list[dict]
) -> str:
    set_a = {frozenset(row.items()) for row in results_a}
    set_b = {frozenset(row.items()) for row in results_b}

    only_in_a = set_a - set_b
    only_in_b = set_b - set_a
    in_both = set_a & set_b

    lines = [
        f"Comparison: {source_a} vs {source_b}",
        f"{'─' * 40}",
        f"Rows in both:        {len(in_both)}",
        f"Only in {source_a}:  {len(only_in_a)}",
        f"Only in {source_b}:  {len(only_in_b)}",
    ]

    if only_in_a:
        lines.append(f"\nRows only in {source_a}:")
        for row in only_in_a:
            lines.append(f"  {dict(row)}")

    if only_in_b:
        lines.append(f"\nRows only in {source_b}:")
        for row in only_in_b:
            lines.append(f"  {dict(row)}")

    if not only_in_a and not only_in_b:
        lines.append("\nBoth sources are identical.")

    return "\n".join(lines)