#!/usr/bin/env python3
"""
MCP Server for Market Data Database

Provides Claude Code access to a market data database using the
marketdata-db-locupleto package.
"""

import os
from typing import Any

import pandas as pd

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Tool,
    TextContent,
    Resource,
)

from marketdata.database import Database


# Environment variables
MARKETDATA_DB_PATH = os.environ.get("MARKETDATA_DB_PATH", "")
EOD_API_KEY = os.environ.get("EOD_API_KEY", "")

# Initialize MCP server
server = Server("marketdata-db")


def get_database() -> Database:
    """Get an opened Database instance."""
    if not MARKETDATA_DB_PATH:
        raise ValueError("MARKETDATA_DB_PATH environment variable not set")
    if not os.path.exists(MARKETDATA_DB_PATH):
        raise FileNotFoundError(f"Database not found: {MARKETDATA_DB_PATH}")

    db = Database()
    # Pass EOD_API_KEY if available, otherwise None
    api_license = EOD_API_KEY if EOD_API_KEY else None
    db.open(database_file=MARKETDATA_DB_PATH, api_license=api_license)
    return db


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="get_eod_data",
            description="Get end-of-day OHLCV price data for a symbol. Returns a pandas DataFrame with date, open, high, low, close, adjusted_close, volume.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "The symbol code (e.g., AAPL, MSFT, VIX)"
                    },
                    "exchange": {
                        "type": "string",
                        "description": "Exchange code (e.g., US, LSE, XETRA, CBOE, SYNTHETICS)",
                        "default": "US"
                    },
                    "adj_for_splits": {
                        "type": "boolean",
                        "description": "Whether to adjust prices for stock splits (default: true)",
                        "default": True
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of rows to return (default 100, most recent first)",
                        "default": 100
                    }
                },
                "required": ["symbol"]
            }
        ),
        Tool(
            name="search_symbols",
            description="Search for symbols in the subscribed symbols list by name or code",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (symbol code or company name)"
                    },
                    "exchange": {
                        "type": "string",
                        "description": "Filter by exchange code (optional)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default 50)",
                        "default": 50
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="list_exchanges",
            description="List all exchanges that have subscribed symbols",
            inputSchema={
                "type": "object",
                "properties": {
                    "include_synthetics": {
                        "type": "boolean",
                        "description": "Include synthetic test exchange (default: false)",
                        "default": False
                    }
                }
            }
        ),
        Tool(
            name="get_subscribed_symbols",
            description="Get list of symbols actively tracked for updates",
            inputSchema={
                "type": "object",
                "properties": {
                    "exchange": {
                        "type": "string",
                        "description": "Filter by exchange code (optional)"
                    },
                    "type": {
                        "type": "string",
                        "description": "Filter by symbol type, e.g., 'Common Stock', 'ETF', 'Index' (optional)"
                    }
                }
            }
        ),
        Tool(
            name="get_symbol_info",
            description="Get sector and industry information for a symbol",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {
                        "type": "string",
                        "description": "The symbol code"
                    },
                    "exchange": {
                        "type": "string",
                        "description": "Exchange code (default: US)",
                        "default": "US"
                    }
                },
                "required": ["symbol"]
            }
        ),
        Tool(
            name="get_database_status",
            description="Get database statistics and status information",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="subscribe_symbol",
            description="Subscribe to a symbol for EOD data updates. All three parameters are required to avoid ambiguity.",
            inputSchema={
                "type": "object",
                "properties": {
                    "exchange_code": {
                        "type": "string",
                        "description": "Exchange code (e.g., US, LSE, XETRA, CBOE)"
                    },
                    "symbol_code": {
                        "type": "string",
                        "description": "The symbol code (e.g., AAPL, MSFT, SPY)"
                    },
                    "type": {
                        "type": "string",
                        "description": "Symbol type (e.g., 'Common Stock', 'ETF', 'Index')"
                    }
                },
                "required": ["exchange_code", "symbol_code", "type"]
            }
        ),
        Tool(
            name="unsubscribe_symbol",
            description="Unsubscribe from a symbol to stop EOD data updates. All three parameters are required to avoid ambiguity.",
            inputSchema={
                "type": "object",
                "properties": {
                    "exchange_code": {
                        "type": "string",
                        "description": "Exchange code (e.g., US, LSE, XETRA, CBOE)"
                    },
                    "symbol_code": {
                        "type": "string",
                        "description": "The symbol code (e.g., AAPL, MSFT, SPY)"
                    },
                    "type": {
                        "type": "string",
                        "description": "Symbol type (e.g., 'Common Stock', 'ETF', 'Index')"
                    }
                },
                "required": ["exchange_code", "symbol_code", "type"]
            }
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls."""
    try:
        if name == "get_eod_data":
            return await handle_get_eod_data(arguments)
        elif name == "search_symbols":
            return await handle_search_symbols(arguments)
        elif name == "list_exchanges":
            return await handle_list_exchanges(arguments)
        elif name == "get_subscribed_symbols":
            return await handle_get_subscribed_symbols(arguments)
        elif name == "get_symbol_info":
            return await handle_get_symbol_info(arguments)
        elif name == "get_database_status":
            return await handle_get_database_status(arguments)
        elif name == "subscribe_symbol":
            return await handle_subscribe_symbol(arguments)
        elif name == "unsubscribe_symbol":
            return await handle_unsubscribe_symbol(arguments)
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def handle_get_eod_data(arguments: dict[str, Any]) -> list[TextContent]:
    """Get EOD price data for a symbol using the Database class."""
    symbol = arguments["symbol"].upper()
    exchange = arguments.get("exchange", "US").upper()
    adj_for_splits = arguments.get("adj_for_splits", True)
    limit = arguments.get("limit", 100)

    db = get_database()
    try:
        # Use the Database class method
        include_synthetics = (exchange == "SYNTHETICS")
        df = db.get_eod_data(
            symbol_code=symbol,
            exchange_code=exchange,
            adj_for_splits=adj_for_splits,
            include_synthetics=include_synthetics
        )

        if df is None or df.empty:
            return [TextContent(type="text", text=f"No data found for {symbol}.{exchange}")]

        # Sort by date descending and limit
        df = df.sort_values('date', ascending=False).head(limit)

        # Format as table
        lines = [f"EOD Data for {symbol}.{exchange} ({len(df)} rows):", ""]
        lines.append("Date       | Open     | High     | Low      | Close    | Volume")
        lines.append("-" * 70)

        for _, row in df.iterrows():
            lines.append(
                f"{row['date']} | {row['open']:8.2f} | {row['high']:8.2f} | "
                f"{row['low']:8.2f} | {row['close']:8.2f} | {int(row['volume']):>10}"
            )

        return [TextContent(type="text", text="\n".join(lines))]
    finally:
        db.close()


async def handle_search_symbols(arguments: dict[str, Any]) -> list[TextContent]:
    """Search for symbols in subscribed_symbols."""
    query = arguments["query"].upper()
    exchange = arguments.get("exchange", "").upper() if arguments.get("exchange") else None
    limit = arguments.get("limit", 50)

    db = get_database()
    try:
        # Get subscribed symbols and filter
        df = db.get_subscribed_symbols(exchange_code=exchange)

        if df.empty:
            return [TextContent(type="text", text=f"No symbols found matching '{query}'")]

        # Filter by query (symbol_code or name)
        mask = (
            df['symbol_code'].str.contains(query, case=False, na=False) |
            df['name'].str.contains(query, case=False, na=False)
        )
        df_filtered = df[mask].head(limit)

        if df_filtered.empty:
            return [TextContent(type="text", text=f"No symbols found matching '{query}'")]

        lines = [f"Symbols matching '{query}' ({len(df_filtered)} results):", ""]
        for _, row in df_filtered.iterrows():
            name = row['name'] if row['name'] else ''
            lines.append(f"{row['symbol_code']}.{row['exchange_code']}: {name[:50]}")

        return [TextContent(type="text", text="\n".join(lines))]
    finally:
        db.close()


async def handle_list_exchanges(arguments: dict[str, Any]) -> list[TextContent]:
    """List all exchanges with subscribed symbols."""
    include_synthetics = arguments.get("include_synthetics", False)

    db = get_database()
    try:
        df = db.get_subscribed_exchanges(include_synthetics=include_synthetics)

        lines = ["Exchanges with Subscribed Symbols:", ""]
        for _, row in df.iterrows():
            lines.append(f"  {row['exchange_code']}")

        return [TextContent(type="text", text="\n".join(lines))]
    finally:
        db.close()


async def handle_get_subscribed_symbols(arguments: dict[str, Any]) -> list[TextContent]:
    """Get subscribed symbols."""
    exchange = arguments.get("exchange", "").upper() if arguments.get("exchange") else None
    symbol_type = arguments.get("type")

    db = get_database()
    try:
        df = db.get_subscribed_symbols(exchange_code=exchange, type=symbol_type)

        lines = [f"Subscribed Symbols ({len(df)} total):", ""]

        current_exchange = ""
        for _, row in df.iterrows():
            if row['exchange_code'] != current_exchange:
                current_exchange = row['exchange_code']
                lines.append(f"\n[{current_exchange}]")
            lines.append(f"  {row['symbol_code']}: {row['name'][:40] if row['name'] else ''}")

        return [TextContent(type="text", text="\n".join(lines))]
    finally:
        db.close()


async def handle_get_symbol_info(arguments: dict[str, Any]) -> list[TextContent]:
    """Get symbol information (sector/industry)."""
    symbol = arguments["symbol"].upper()
    exchange = arguments.get("exchange", "US").upper()

    db = get_database()
    try:
        # Get symbol info using the Database class method
        info = db.get_symbol_info(symbol, exchange)

        # Also get the name from subscribed_symbols
        df = db.get_subscribed_symbols(exchange_code=exchange)
        symbol_row = df[df['symbol_code'] == symbol]
        name = symbol_row['name'].iloc[0] if not symbol_row.empty else 'N/A'

        lines = [
            f"Symbol: {symbol}.{exchange}",
            f"Name: {name}",
            f"Sector: {info.get('sector', 'N/A')}",
            f"Industry: {info.get('industry', 'N/A')}"
        ]

        return [TextContent(type="text", text="\n".join(lines))]
    finally:
        db.close()


async def handle_get_database_status(arguments: dict[str, Any]) -> list[TextContent]:
    """Get database status and statistics."""
    db = get_database()
    try:
        # Get counts using the Database's sql method
        exchanges_df = db.get_subscribed_exchanges()
        num_exchanges = len(exchanges_df)

        symbols_df = db.get_subscribed_symbols()
        num_symbols = len(symbols_df)

        # Get date range from subscribed_symbols
        if not symbols_df.empty:
            first_dates = symbols_df['first_date'].dropna()
            last_dates = symbols_df['last_date'].dropna()
            min_date = first_dates.min() if not first_dates.empty else 'N/A'
            max_date = last_dates.max() if not last_dates.empty else 'N/A'
        else:
            min_date = 'N/A'
            max_date = 'N/A'

        # Database file size
        db_size = os.path.getsize(MARKETDATA_DB_PATH) / (1024 * 1024)  # MB

        # Check API key status
        api_key_status = "Configured" if EOD_API_KEY else "Not set"

        lines = [
            "Market Data Database Status",
            "=" * 40,
            f"Database Path: {MARKETDATA_DB_PATH}",
            f"Database Size: {db_size:.1f} MB",
            f"EOD API Key: {api_key_status}",
            f"Package Version: {db.version}",
            "",
            "Statistics:",
            f"  Exchanges with subscriptions: {num_exchanges}",
            f"  Subscribed Symbols: {num_symbols}",
            "",
            f"Date Range: {min_date} to {max_date}",
        ]

        return [TextContent(type="text", text="\n".join(lines))]
    finally:
        db.close()


async def handle_subscribe_symbol(arguments: dict[str, Any]) -> list[TextContent]:
    """Subscribe to a symbol for EOD data updates."""
    exchange_code = arguments["exchange_code"].upper()
    symbol_code = arguments["symbol_code"].upper()
    symbol_type = arguments["type"]

    db = get_database()
    try:
        # Verify the symbol exists in subscribed_symbols table
        df_all = db.get_subscribed_symbols(exchange_code=exchange_code)
        symbol_row = df_all[
            (df_all['symbol_code'] == symbol_code) &
            (df_all['type'] == symbol_type)
        ]

        if symbol_row.empty:
            return [TextContent(
                type="text",
                text=f"Symbol not found: {exchange_code}/{symbol_type}/{symbol_code}\n"
                     f"The symbol must exist in the exchange_symbols table first."
            )]

        # Check if already subscribed
        if symbol_row['is_subscribed'].iloc[0] != 0:
            return [TextContent(
                type="text",
                text=f"Already subscribed: {exchange_code}/{symbol_type}/{symbol_code}"
            )]

        # Create DataFrame for start_subscriptions
        df_subscribe = pd.DataFrame([{
            'symbol_code': symbol_code,
            'exchange_code': exchange_code
        }])

        db.start_subscriptions(df_subscribe)

        return [TextContent(
            type="text",
            text=f"Subscribed: {exchange_code}/{symbol_type}/{symbol_code}\n"
                 f"EOD data will be updated on next refresh."
        )]
    finally:
        db.close()


async def handle_unsubscribe_symbol(arguments: dict[str, Any]) -> list[TextContent]:
    """Unsubscribe from a symbol to stop EOD data updates."""
    exchange_code = arguments["exchange_code"].upper()
    symbol_code = arguments["symbol_code"].upper()
    symbol_type = arguments["type"]

    db = get_database()
    try:
        # Verify the symbol exists and is subscribed
        df_all = db.get_subscribed_symbols(exchange_code=exchange_code)
        symbol_row = df_all[
            (df_all['symbol_code'] == symbol_code) &
            (df_all['type'] == symbol_type)
        ]

        if symbol_row.empty:
            return [TextContent(
                type="text",
                text=f"Symbol not found: {exchange_code}/{symbol_type}/{symbol_code}"
            )]

        # Check if already unsubscribed
        if symbol_row['is_subscribed'].iloc[0] == 0:
            return [TextContent(
                type="text",
                text=f"Already unsubscribed: {exchange_code}/{symbol_type}/{symbol_code}"
            )]

        # Create DataFrame for stop_subscriptions
        df_unsubscribe = pd.DataFrame([{
            'symbol_code': symbol_code,
            'exchange_code': exchange_code
        }])

        db.stop_subscriptions(df_unsubscribe)

        return [TextContent(
            type="text",
            text=f"Unsubscribed: {exchange_code}/{symbol_type}/{symbol_code}\n"
                 f"EOD data will no longer be updated."
        )]
    finally:
        db.close()


@server.list_resources()
async def list_resources() -> list[Resource]:
    """List available resources."""
    return [
        Resource(
            uri="marketdata://status",
            name="Database Status",
            description="Current database statistics and status",
            mimeType="text/plain"
        ),
        Resource(
            uri="marketdata://exchanges",
            name="Exchange List",
            description="List of exchanges with subscribed symbols",
            mimeType="text/plain"
        ),
    ]


@server.read_resource()
async def read_resource(uri: str) -> str:
    """Read a resource."""
    if uri == "marketdata://status":
        result = await handle_get_database_status({})
        return result[0].text
    elif uri == "marketdata://exchanges":
        result = await handle_list_exchanges({})
        return result[0].text
    else:
        raise ValueError(f"Unknown resource: {uri}")


async def main():
    """Run the MCP server."""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
