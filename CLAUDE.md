# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Model Context Protocol (MCP) server that provides access to a market data SQLite database via the `marketdata-db-locupleto` package. It integrates with Claude Code to enable querying financial market data including EOD prices, symbols, exchanges, and subscription management.

## Prerequisites

- Python 3.10+
- Access to marketdata.db (path set via `MARKETDATA_DB_PATH` environment variable)
- EOD API key (set via `EOD_API_KEY` environment variable)

## Environment Variables

```bash
# Required: Path to the SQLite market data database
export MARKETDATA_DB_PATH=/Volumes/Work/marketdata/marketdata.db

# Required: EOD Historical Data API key
export EOD_API_KEY=your_api_key_here
```

## Development Setup

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Register with Claude Code (user level - available in all projects)
claude mcp add -s user marketdata-db \
    '/Volumes/Work/development/projects/git/mcp-marketdata-db/venv/bin/python3' \
    '/Volumes/Work/development/projects/git/mcp-marketdata-db/marketdata_mcp_server.py'

# Verify registration
claude mcp list

# Debug mode for troubleshooting
claude --mcp-debug
```

## Architecture

The server (`marketdata_mcp_server.py`) is a single-file MCP server using `mcp.server` and the `marketdata-db-locupleto` package (NOT raw SQLite queries).

### Symbol Identification - The Triplet Rule

**IMPORTANT**: All symbol-specific operations require the full triplet to avoid ambiguity:
- `exchange_code`: e.g., US, LSE, XETRA, CBOE
- `symbol_code`: e.g., AAPL, SPY, VIX
- `type`: e.g., 'Common Stock', 'ETF', 'Index'

Use `list_exchanges` or `get_symbol_types` to discover valid type values.

### Tools (10 total)

**Data Retrieval:**
- `get_eod_data`: Get OHLCV price data (requires triplet)
- `search_symbols`: Search symbols by name or code
- `get_symbol_info`: Get sector/industry info (requires triplet)

**Discovery:**
- `list_exchanges`: List exchanges with their available symbol types
- `get_symbol_types`: Get distinct types for a specific exchange
- `get_subscribed_symbols`: List actively tracked symbols

**Status:**
- `get_database_status`: Database statistics (size, counts, date range)
- `get_update_status`: EOD data freshness per exchange/type

**Subscription Management:**
- `subscribe_symbol`: Start tracking a symbol (requires triplet)
- `unsubscribe_symbol`: Stop tracking a symbol (requires triplet)

### Resources

- `marketdata://status`: Database status and statistics
- `marketdata://exchanges`: List of exchanges with symbol types

## Database Schema

The SQLite database contains these key tables:
- `exchanges`: Market exchange information
- `exchange_symbols`: Available symbols per exchange
- `subscribed_symbols`: Symbols actively tracked for updates
- `eod_data`: End-of-day price data (OHLCV)
- `splits`: Stock split information
- `symbol_info`: Sector and industry classification

## Dependencies

Core dependencies (see `requirements.txt`):
- `mcp`: MCP SDK
- `marketdata-db-locupleto`: Market data database package
- `yfinance`: Yahoo Finance data
- `eod`: EOD Historical Data API

## Related Projects

- `marketdata-db`: The core Python package that manages the database
- `marketdata-db-app`: Streamlit frontend application
