# mcp-marketdata-db

A Model Context Protocol (MCP) server that provides Claude Code access to a market data SQLite database.

## Features

- Query end-of-day (EOD) price data for stocks and indices
- Search symbols across multiple exchanges
- Access sector and industry classifications
- View index constituents
- Get database statistics and status

## Installation

```bash
# Clone the repository
git clone https://github.com/locupleto/mcp-marketdata-db.git
cd mcp-marketdata-db

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install mcp
```

## Configuration

Set the required environment variables:

```bash
export MARKETDATA_DB_PATH=/path/to/marketdata.db
export EOD_API_KEY=your_api_key  # Optional, for live updates
```

## Register with Claude Code

```bash
claude mcp add -s user marketdata-db \
    "$(pwd)/venv/bin/python3" \
    "$(pwd)/marketdata_mcp_server.py"
```

## Usage

Once registered, Claude Code will have access to market data tools:

- **get_eod_data**: Get OHLCV data for any symbol
- **search_symbols**: Find symbols by name or code
- **list_exchanges**: See available exchanges
- **get_subscribed_symbols**: View tracked symbols
- **get_symbol_info**: Get sector/industry data

## Requirements

- Python 3.10+
- MCP SDK
- SQLite market data database (from marketdata-db project)

## License

MIT
