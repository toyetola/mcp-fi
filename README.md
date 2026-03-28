# MCP-FI

**Query any database in plain English using Claude.**

MCP-FI is a [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server that connects Claude to your data sources — SQLite, CSV, PostgreSQL, and MySQL. Instead of writing SQL, you just ask questions and Claude figures out the rest.

---

## How It Works

```
You: "Which customers have the highest lifetime value?"
        ↓
Claude calls summarise_data → understands your schema
        ↓
Claude calls query_data → generates and runs the SQL
        ↓
Claude explains the results in plain English
```

No SQL knowledge required.

---

## Features

- **Multiple data sources** — SQLite, CSV, PostgreSQL, MySQL
- **Three tools** exposed to Claude:
  - `summarise_data` — understand table structure before querying
  - `query_data` — run SQL against any configured source
  - `compare_data` — diff two sources and find differences
- **Plain English queries** — Claude writes the SQL for you
- **Local or deployed** — run locally via Claude Desktop or deploy via SSE for others to use

---

## Requirements

- Python 3.12+
- [Claude Desktop](https://claude.ai/download) (for local use)
- Node.js (for MCP Inspector testing)

---

## Installation

**1. Clone the repo:**
```bash
git clone https://github.com/yourusername/mcp-fi.git
cd mcp-fi
```

**2. Create and activate a virtual environment:**
```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows
```

**3. Install dependencies:**
```bash
pip install -r requirements.txt
```

**4. Set up your config:**
```bash
cp config.example.json config.json
```

Then edit `config.json` with your actual data source paths and credentials (see Configuration below).

---

## Configuration

Edit `config.json` to define your data sources. Each source needs a unique name that you'll use when asking Claude questions.

```json
{
  "sources": {
    "my_sqlite_db": {
      "type": "sqlite",
      "path": "/absolute/path/to/your/database.sqlite"
    },
    "my_csv_data": {
      "type": "csv",
      "data_dir": "/absolute/path/to/your/csv/folder/"
    },
    "my_postgres": {
      "type": "postgres",
      "dsn": "postgresql://user:password@localhost/dbname"
    },
    "my_mysql": {
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "database": "mydb",
      "user": "root",
      "password": "yourpassword"
    }
  }
}
```

**Important:** Always use absolute paths — relative paths will not work when Claude Desktop launches the server.

You can define as many sources as you need. Mix and match types freely.

---

## Connect to Claude Desktop

Open your Claude Desktop config file:

**Mac:**
```bash
open ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

**Windows:**
```
%APPDATA%\Claude\claude_desktop_config.json
```

Add the `mcpServers` block:

```json
{
  "mcpServers": {
    "mcp-fi": {
      "command": "/absolute/path/to/mcp-fi/venv/bin/python",
      "args": [
        "/absolute/path/to/mcp-fi/server.py"
      ],
      "cwd": "/absolute/path/to/mcp-fi"
    }
  }
}
```

Restart Claude Desktop. You should see a 🔨 hammer icon in the chat input — that confirms MCP-FI is connected.

---

## Usage Examples

Once connected, just ask Claude questions in plain English:

**Explore your data:**
```
What tables are available in my_sqlite_db?
```

**Query data:**
```
Show me the top 10 customers by total orders from my_postgres
```

**Filter and aggregate:**
```
What is the average net worth by income level in my_csv_data?
```

**Compare sources:**
```
Compare the users table between my_sqlite_db and my_postgres — are they in sync?
```

**Complex analysis:**
```
Which products in my_mysql have been ordered more than 100 times 
and what is their average rating?
```

Claude will automatically call `summarise_data` to understand your schema, then `query_data` to fetch results, and finally explain everything back to you clearly.

---

## Testing

Run the full test suite:

```bash
pytest tests/ -v
```

Run a specific connector test:

```bash
pytest tests/test_sqlite_connector.py -v
pytest tests/test_csv_connector.py -v
pytest tests/test_postgres_connector.py -v
pytest tests/test_mysql_connector.py -v
```

The suite covers 116 tests across all connectors, tools, router, and server — all using isolated temporary databases so no real data is touched.

---

## Test With MCP Inspector

Before connecting to Claude Desktop, you can test all tools interactively:

```bash
npx @modelcontextprotocol/inspector python server.py
```

Open the URL shown in your terminal, click **Connect**, then **Tools** to see and run all three tools manually.

---

## Deployment (SSE)

To deploy MCP-FI as a shared server others can connect to:

**1. Run in SSE mode:**
```bash
python server.py --sse --port 8000
```

**2. Deploy to Railway:**

Create a `Procfile`:
```
web: python server.py --sse --port $PORT
```

Push to GitHub, connect the repo to [Railway](https://railway.app), and deploy. You'll get a public URL like:
```
https://mcp-fi-production.up.railway.app
```

**3. Others connect to your server** by adding this to their `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "mcp-fi": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "https://mcp-fi-production.up.railway.app/sse"
      ]
    }
  }
}
```

---

## Project Structure

```
mcp-fi/
├── connectors/
│   ├── base_connector.py      # Abstract contract all connectors implement
│   ├── sqlite_connector.py    # SQLite support
│   ├── csv_connector.py       # CSV support via DuckDB
│   ├── postgres_connector.py  # PostgreSQL support
│   ├── mysql_connector.py     # MySQL support
│   └── router.py              # Routes source names to correct connector
├── tools/
│   ├── query_tool.py          # Run SQL against any source
│   ├── summarise_tool.py      # Describe table structure
│   └── compare_tool.py        # Diff two sources
├── tests/                     # 116 tests across all layers
├── data/                      # Put your SQLite and CSV files here
├── server.py                  # Entry point — stdio and SSE transport
├── config.json                # Your data sources (gitignored)
├── config.example.json        # Template for config.json
└── requirements.txt           # Python dependencies
```

---

## Adding a New Data Source

1. Add your source to `config.json`
2. Ask Claude: *"What tables are in my_new_source?"*

That's it. No code changes needed.

---

## Supported Data Sources

| Type | Config key | Notes |
|---|---|---|
| SQLite | `"type": "sqlite"` | Provide absolute `path` to `.sqlite` file |
| CSV | `"type": "csv"` | Provide absolute `data_dir` — each CSV becomes a table |
| PostgreSQL | `"type": "postgres"` | Provide `dsn` connection string |
| MySQL | `"type": "mysql"` | Provide `host`, `port`, `database`, `user`, `password` |

---

## License

MIT