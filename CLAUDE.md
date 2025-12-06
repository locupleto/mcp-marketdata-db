# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Model Context Protocol (MCP) server that provides access to a market data SQLite database. It integrates with Claude Code to enable querying financial market data including EOD prices, symbols, exchanges, and index constituents.

## Prerequisites

- Python 3.10+
- MCP SDK: `pip install mcp`
- Access to marketdata.db (path set via `MARKETDATA_DB_PATH` environment variable)
- EOD API key (set via `EOD_API_KEY` environment variable) - optional, for live data updates

## Environment Variables

```bash
# Required: Path to the SQLite market data database
export MARKETDATA_DB_PATH=/Volumes/Work/marketdata/marketdata.db

# Optional: EOD Historical Data API key for live updates
export EOD_API_KEY=your_api_key_here
```

## Development Setup

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install mcp

# Register with Claude Code (use absolute paths)
claude mcp add -s user marketdata-db '/absolute/path/to/venv/bin/python3' '/absolute/path/to/marketdata_mcp_server.py'

# Verify registration
claude mcp list

# Debug mode for troubleshooting
claude --mcp-debug
```

## Architecture

The server (`marketdata_mcp_server.py`) is a single-file MCP server implementation using `mcp.server` that provides:

**Tools:**
- `get_eod_data`: Retrieve end-of-day OHLCV data for a symbol
- `search_symbols`: Search for symbols by name or code
- `list_exchanges`: List available exchanges in the database
- `get_subscribed_symbols`: Get list of actively tracked symbols
- `get_symbol_info`: Get sector/industry information for a symbol

**Resources:**
- `marketdata://schema`: Database schema information
- `marketdata://exchanges`: List of available exchanges
- `marketdata://status`: Database status and statistics

## Database Schema

The SQLite database contains these key tables:
- `exchanges`: Market exchange information
- `exchange_symbols`: Available symbols per exchange
- `subscribed_symbols`: Symbols actively tracked for updates
- `eod_data`: End-of-day price data (OHLCV)
- `splits`: Stock split information
- `symbol_info`: Sector and industry classification

## Related Projects

- `marketdata-db`: The core Python package that manages the database
- `marketdata-db-app`: Streamlit frontend application
