# mcp-marketdata-db

A Model Context Protocol (MCP) server that provides Claude Code access to a market data SQLite database.

## Features

- Query end-of-day (EOD) price data for stocks, ETFs, and indices
- Search symbols across multiple exchanges (US, LSE, XETRA, CBOE, etc.)
- Discover available symbol types per exchange
- Check data freshness and update status
- Manage symbol subscriptions
- Access sector and industry classifications

## Installation

```bash
# Clone the repository
git clone https://github.com/locupleto/mcp-marketdata-db.git
cd mcp-marketdata-db

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Set the required environment variables:

```bash
export MARKETDATA_DB_PATH=/path/to/marketdata.db
export EOD_API_KEY=your_api_key
```

## Register with Claude Code

```bash
# User level (available in all projects)
claude mcp add -s user marketdata-db \
    "$(pwd)/venv/bin/python3" \
    "$(pwd)/marketdata_mcp_server.py"

# Or project level (current project only)
claude mcp add marketdata-db \
    "$(pwd)/venv/bin/python3" \
    "$(pwd)/marketdata_mcp_server.py"
```

## Tools (10 total)

### Symbol Triplet Requirement

Symbol-specific tools require all three identifiers to avoid ambiguity:
- `exchange_code`: US, LSE, XETRA, CBOE, etc.
- `symbol_code`: AAPL, SPY, VIX, etc.
- `type`: 'Common Stock', 'ETF', 'Index', etc.

### Data Retrieval
| Tool | Description |
|------|-------------|
| `get_eod_data` | Get OHLCV price data for a symbol (requires triplet) |
| `search_symbols` | Find symbols by name or code |
| `get_symbol_info` | Get sector/industry data (requires triplet) |

### Discovery
| Tool | Description |
|------|-------------|
| `list_exchanges` | List exchanges with available symbol types |
| `get_symbol_types` | Get distinct types for a specific exchange |
| `get_subscribed_symbols` | View tracked symbols |

### Status
| Tool | Description |
|------|-------------|
| `get_database_status` | Database size, counts, date range |
| `get_update_status` | EOD data freshness per exchange/type |

### Subscription Management
| Tool | Description |
|------|-------------|
| `subscribe_symbol` | Start tracking a symbol (requires triplet) |
| `unsubscribe_symbol` | Stop tracking a symbol (requires triplet) |

## Example Usage

```
# First, discover available types
> list_exchanges

[US] (1746 symbols)
  Types: Common Stock, ETF

[CBOE] (22 symbols)
  Types: Index

# Then query with full triplet
> get_eod_data exchange_code=US symbol_code=SPY type=ETF limit=5
```

## Requirements

- Python 3.10+
- marketdata-db-locupleto package
- MCP SDK
- SQLite market data database (from marketdata-db project)

## License

MIT
