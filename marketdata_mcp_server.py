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
            description="Get end-of-day OHLCV price data for a symbol. All three identifiers (exchange_code, symbol_code, type) are required to avoid ambiguity.",
            inputSchema={
                "type": "object",
                "properties": {
                    "exchange_code": {
                        "type": "string",
                        "description": "Exchange code (e.g., US, LSE, XETRA, CBOE, SYNTHETICS)"
                    },
                    "symbol_code": {
                        "type": "string",
                        "description": "The symbol code (e.g., AAPL, MSFT, VIX)"
                    },
                    "type": {
                        "type": "string",
                        "description": "Symbol type (e.g., 'Common Stock', 'ETF', 'Index')"
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
                "required": ["exchange_code", "symbol_code", "type"]
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
            description="List all exchanges that have subscribed symbols, showing the available symbol types in each exchange",
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
            name="get_symbol_types",
            description="Get the distinct symbol types available in a specific exchange. Use this to discover valid type values before querying symbols.",
            inputSchema={
                "type": "object",
                "properties": {
                    "exchange_code": {
                        "type": "string",
                        "description": "Exchange code (e.g., US, LSE, XETRA, CBOE)"
                    }
                },
                "required": ["exchange_code"]
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
            description="Get sector and industry information for a symbol. All three identifiers are required to avoid ambiguity.",
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
            name="get_database_status",
            description="Get database statistics and status information",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_update_status",
            description="Get the EOD data update status showing latest date and percentage of symbols updated per exchange and type. Useful for checking if market data is current.",
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
        Tool(
            name="get_intraday_quote",
            description="Get live (15-20 min delayed) intraday quote for a symbol. Returns current price, bid/ask, volume, and other real-time data. Requires EOD_API_KEY environment variable to be set.",
            inputSchema={
                "type": "object",
                "properties": {
                    "exchange_code": {
                        "type": "string",
                        "description": "Exchange code (e.g., US, LSE, XETRA)"
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
        elif name == "get_symbol_types":
            return await handle_get_symbol_types(arguments)
        elif name == "get_subscribed_symbols":
            return await handle_get_subscribed_symbols(arguments)
        elif name == "get_symbol_info":
            return await handle_get_symbol_info(arguments)
        elif name == "get_database_status":
            return await handle_get_database_status(arguments)
        elif name == "get_update_status":
            return await handle_get_update_status(arguments)
        elif name == "subscribe_symbol":
            return await handle_subscribe_symbol(arguments)
        elif name == "unsubscribe_symbol":
            return await handle_unsubscribe_symbol(arguments)
        elif name == "get_intraday_quote":
            return await handle_get_intraday_quote(arguments)
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def handle_get_eod_data(arguments: dict[str, Any]) -> list[TextContent]:
    """Get EOD price data for a symbol using the Database class."""
    exchange_code = arguments["exchange_code"].upper()
    symbol_code = arguments["symbol_code"].upper()
    symbol_type = arguments["type"]
    adj_for_splits = arguments.get("adj_for_splits", True)
    limit = arguments.get("limit", 100)

    db = get_database()
    try:
        # Verify the symbol exists with the correct type
        df_symbols = db.get_subscribed_symbols(exchange_code=exchange_code)
        symbol_row = df_symbols[
            (df_symbols['symbol_code'] == symbol_code) &
            (df_symbols['type'] == symbol_type)
        ]

        if symbol_row.empty:
            return [TextContent(
                type="text",
                text=f"Symbol not found: {exchange_code}/{symbol_type}/{symbol_code}"
            )]

        # Use the Database class method
        include_synthetics = (exchange_code == "SYNTHETICS")
        df = db.get_eod_data(
            symbol_code=symbol_code,
            exchange_code=exchange_code,
            adj_for_splits=adj_for_splits,
            include_synthetics=include_synthetics
        )

        if df is None or df.empty:
            return [TextContent(
                type="text",
                text=f"No EOD data found for {exchange_code}/{symbol_type}/{symbol_code}"
            )]

        # Sort by date descending and limit (use index if date is the index)
        df = df.sort_index(ascending=False).head(limit)

        # Format as table
        lines = [f"EOD Data for {exchange_code}/{symbol_type}/{symbol_code} ({len(df)} rows):", ""]
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
    """List all exchanges with subscribed symbols and their available types."""
    include_synthetics = arguments.get("include_synthetics", False)

    db = get_database()
    try:
        exchanges_df = db.get_subscribed_exchanges(include_synthetics=include_synthetics)
        symbols_df = db.get_subscribed_symbols(include_synthetics=include_synthetics)

        lines = ["Exchanges with Subscribed Symbols:", ""]

        for _, row in exchanges_df.iterrows():
            exchange_code = row['exchange_code']
            # Get distinct types for this exchange
            exchange_symbols = symbols_df[symbols_df['exchange_code'] == exchange_code]
            types = sorted(exchange_symbols['type'].dropna().unique().tolist())
            symbol_count = len(exchange_symbols)

            lines.append(f"[{exchange_code}] ({symbol_count} symbols)")
            lines.append(f"  Types: {', '.join(types)}")
            lines.append("")

        return [TextContent(type="text", text="\n".join(lines))]
    finally:
        db.close()


async def handle_get_symbol_types(arguments: dict[str, Any]) -> list[TextContent]:
    """Get distinct symbol types for a specific exchange."""
    exchange_code = arguments["exchange_code"].upper()

    db = get_database()
    try:
        # Check if exchange exists
        include_synthetics = (exchange_code == "SYNTHETICS")
        symbols_df = db.get_subscribed_symbols(
            exchange_code=exchange_code,
            include_synthetics=include_synthetics
        )

        if symbols_df.empty:
            return [TextContent(
                type="text",
                text=f"No subscribed symbols found for exchange: {exchange_code}"
            )]

        # Get distinct types with counts
        type_counts = symbols_df['type'].value_counts().sort_index()

        lines = [f"Symbol Types in {exchange_code}:", ""]
        for symbol_type, count in type_counts.items():
            lines.append(f"  {symbol_type}: {count} symbols")

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
    exchange_code = arguments["exchange_code"].upper()
    symbol_code = arguments["symbol_code"].upper()
    symbol_type = arguments["type"]

    db = get_database()
    try:
        # Verify the symbol exists with the correct type
        df = db.get_subscribed_symbols(exchange_code=exchange_code)
        symbol_row = df[
            (df['symbol_code'] == symbol_code) &
            (df['type'] == symbol_type)
        ]

        if symbol_row.empty:
            return [TextContent(
                type="text",
                text=f"Symbol not found: {exchange_code}/{symbol_type}/{symbol_code}"
            )]

        name = symbol_row['name'].iloc[0] if symbol_row['name'].iloc[0] else 'N/A'

        # Get symbol info using the Database class method
        info = db.get_symbol_info(symbol_code, exchange_code)

        lines = [
            f"Symbol: {exchange_code}/{symbol_type}/{symbol_code}",
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


async def handle_get_update_status(arguments: dict[str, Any]) -> list[TextContent]:
    """Get EOD data update status per exchange and type."""
    db = get_database()
    try:
        # SQL query to fetch latest update date and percentage of updated symbols
        update_status_sql = '''
        WITH LatestUpdates AS (
            SELECT
                exchange_code,
                MAX(date) AS latest_date
            FROM eod_data
            WHERE (symbol_code, exchange_code) IN (
                SELECT symbol_code, exchange_code
                FROM subscribed_symbols
                WHERE is_subscribed = 1
            )
            GROUP BY exchange_code
        ),
        ExchangeSummary AS (
            SELECT
                ex.name,
                ex.code AS exchange_code,
                lu.latest_date,
                ss.type,
                COUNT(ed.symbol_code) AS updated_count,
                COUNT(ss.symbol_code) AS total_count
            FROM exchanges ex
            JOIN LatestUpdates lu ON ex.code = lu.exchange_code
            JOIN subscribed_symbols ss ON ss.exchange_code = ex.code AND ss.is_subscribed = 1
            LEFT JOIN eod_data ed ON ed.symbol_code = ss.symbol_code
                AND ed.exchange_code = ss.exchange_code
                AND ed.date = lu.latest_date
            GROUP BY ex.name, ex.code, lu.latest_date, ss.type
        )
        SELECT
            name AS exchange_name,
            type AS symbol_type,
            total_count AS symbols,
            latest_date,
            ROUND(100.0 * updated_count / total_count, 2) AS percentage_updated
        FROM ExchangeSummary
        ORDER BY exchange_name, symbol_type;
        '''

        df = db.sql(update_status_sql)

        if df is None or df.empty:
            return [TextContent(type="text", text="No update status data available")]

        lines = [
            "EOD Data Update Status",
            "=" * 80,
            f"{'Exchange':<25} {'Type':<15} {'Symbols':<10} {'Latest Date':<15} {'Updated':>10}",
            "-" * 80,
        ]

        for _, row in df.iterrows():
            lines.append(
                f"{row['exchange_name']:<25} {row['symbol_type']:<15} "
                f"{row['symbols']:<10} {row['latest_date']:<15} "
                f"{row['percentage_updated']:>9.1f}%"
            )

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


async def handle_get_intraday_quote(arguments: dict[str, Any]) -> list[TextContent]:
    """Get live (delayed) intraday quote for a symbol."""
    exchange_code = arguments["exchange_code"].upper()
    symbol_code = arguments["symbol_code"].upper()
    symbol_type = arguments["type"]

    # Check if API key is configured
    if not EOD_API_KEY:
        return [TextContent(
            type="text",
            text="Error: EOD_API_KEY environment variable is not set.\n"
                 "Intraday quotes require a valid EOD Historical Data API key."
        )]

    db = get_database()
    try:
        # Verify the symbol exists with the correct type
        df_symbols = db.get_subscribed_symbols(exchange_code=exchange_code)
        symbol_row = df_symbols[
            (df_symbols['symbol_code'] == symbol_code) &
            (df_symbols['type'] == symbol_type)
        ]

        if symbol_row.empty:
            return [TextContent(
                type="text",
                text=f"Symbol not found: {exchange_code}/{symbol_type}/{symbol_code}"
            )]

        # Get intraday quote using the Database class method
        df = db.get_intraday_price(symbol_code, exchange_code)

        if df is None or df.empty:
            return [TextContent(
                type="text",
                text=f"No intraday data available for {exchange_code}/{symbol_type}/{symbol_code}"
            )]

        # Format as readable output
        symbol_name = symbol_row['name'].iloc[0] if symbol_row['name'].iloc[0] else symbol_code

        lines = [
            f"Intraday Quote: {symbol_code}.{exchange_code}",
            f"Name: {symbol_name}",
            "=" * 40,
        ]

        # Extract key fields from the DataFrame (which has 'Column' as index and 'Value' as column)
        for idx, row in df.iterrows():
            # Format the field name nicely
            field_name = str(idx).replace('_', ' ').title()
            value = row['Value']
            lines.append(f"{field_name}: {value}")

        lines.append("")
        lines.append("Note: Data is delayed 15-20 minutes")

        return [TextContent(type="text", text="\n".join(lines))]
    except Exception as e:
        return [TextContent(
            type="text",
            text=f"Error fetching intraday quote: {str(e)}"
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
