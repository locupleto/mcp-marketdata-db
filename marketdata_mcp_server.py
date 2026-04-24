#!/usr/bin/env python3
"""
MCP Server for Market Data Database

Provides Claude Code access to a market data database using the
marketdata-db-locupleto package.
"""

import os
import json
import difflib
from typing import Any, List, Tuple, Optional

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


# Fuzzy matching utilities for symbol validation
def fuzzy_match_symbols(query: str, options: List[str], cutoff: float = 0.6) -> List[Tuple[str, float]]:
    """
    Fuzzy match query against list of symbol codes or names.
    Returns list of (match, confidence_score) tuples sorted by confidence.
    """
    matches = difflib.get_close_matches(query, options, n=5, cutoff=cutoff)
    scored_matches = []
    for match in matches:
        score = difflib.SequenceMatcher(None, query.lower(), match.lower()).ratio()
        scored_matches.append((match, score))
    return sorted(scored_matches, key=lambda x: x[1], reverse=True)


def detect_swedish_company(query: str) -> bool:
    """
    Detect if query likely refers to a Swedish company.
    Heuristics: ends with AB, contains Swedish company names, etc.
    """
    swedish_indicators = [
        'AB', 'Ericsson', 'Volvo', 'H&M', 'Hennes', 'Mauritz',
        'Electrolux', 'Sandvik', 'Atlas Copco', 'ABB', 'Alfa Laval'
    ]
    query_upper = query.upper()
    return any(indicator.upper() in query_upper for indicator in swedish_indicators)


def prioritize_matches(matches: List[dict], prefer_us: bool = True) -> List[dict]:
    """
    Prioritize symbol matches based on exchange and type.

    Priority order (if prefer_us=True):
    1. US Common Stock
    2. US ETF
    3. CBOE Index
    4. ST (Sweden) stocks
    5. Others

    If prefer_us=False (Swedish query detected):
    1. ST stocks
    2. US Common Stock
    3. US ETF
    4. Others
    """
    def priority_score(match: dict) -> int:
        exchange = match.get('exchange_code', '')
        sym_type = match.get('type', '')

        if prefer_us:
            if exchange == 'US' and sym_type == 'Common Stock':
                return 100
            elif exchange == 'US' and sym_type == 'ETF':
                return 90
            elif exchange == 'CBOE' and sym_type == 'Index':
                return 80
            elif exchange == 'ST':
                return 70
            else:
                return 50
        else:
            # Swedish companies prioritized
            if exchange == 'ST':
                return 100
            elif exchange == 'US' and sym_type == 'Common Stock':
                return 90
            elif exchange == 'US' and sym_type == 'ETF':
                return 80
            else:
                return 50

    return sorted(matches, key=priority_score, reverse=True)


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
            name="validate_symbol",
            description=(
                "Validate a symbol query with fuzzy matching and smart defaults. "
                "Handles typos, case-insensitive matching, company names, and ticker symbols. "
                "Returns best match with confidence score and suggestions."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Symbol code or company name (e.g., 'aapl', 'Apple', 'spy')"
                    },
                    "return_suggestions": {
                        "type": "boolean",
                        "description": "Return top 5 suggestions even if confidence < 100% (default: true)",
                        "default": True
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
                    },
                    "return_json": {
                        "type": "boolean",
                        "description": "If true, return structured JSON response (for n8n integration). Default: false",
                        "default": False
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
        elif name == "validate_symbol":
            return await handle_validate_symbol(arguments)
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


async def handle_validate_symbol(arguments: dict[str, Any]) -> list[TextContent]:
    """
    Validate a symbol query with fuzzy matching and smart defaults.

    Args:
        query: Symbol code or company name (case-insensitive)
        return_suggestions: If True, return top matches even if not perfect

    Returns:
        JSON with:
        {
            "valid": bool,
            "matched_symbol": str,  # Best match symbol code
            "exchange_code": str,
            "type": str,
            "name": str,  # Full company name
            "confidence": float,  # 0.0-1.0
            "suggestions": [  # If confidence < 1.0
                {"symbol_code": str, "exchange_code": str, "type": str, "name": str, "confidence": float}
            ]
        }
    """
    query = arguments["query"]
    return_suggestions = arguments.get("return_suggestions", True)

    db = get_database()
    try:
        # Get all subscribed symbols
        all_symbols = db.get_subscribed_symbols()

        if all_symbols.empty:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "valid": False,
                    "error": "No symbols available in database",
                    "confidence": 0.0
                }, indent=2)
            )]

        # Detect if query is likely Swedish
        is_swedish_query = detect_swedish_company(query)

        # Search by symbol code (exact match first)
        exact_symbol_match = all_symbols[
            all_symbols['symbol_code'].str.upper() == query.upper()
        ]

        if not exact_symbol_match.empty:
            # Exact match found - prioritize
            matches = prioritize_matches(
                exact_symbol_match.to_dict('records'),
                prefer_us=not is_swedish_query
            )
            best_match = matches[0]

            return [TextContent(
                type="text",
                text=json.dumps({
                    "valid": True,
                    "matched_symbol": best_match['symbol_code'],
                    "exchange_code": best_match['exchange_code'],
                    "type": best_match['type'],
                    "name": best_match['name'],
                    "confidence": 1.0,
                    "suggestions": []
                }, indent=2)
            )]

        # Search by company name (exact match)
        exact_name_match = all_symbols[
            all_symbols['name'].str.upper() == query.upper()
        ]

        if not exact_name_match.empty:
            matches = prioritize_matches(
                exact_name_match.to_dict('records'),
                prefer_us=not is_swedish_query
            )
            best_match = matches[0]

            return [TextContent(
                type="text",
                text=json.dumps({
                    "valid": True,
                    "matched_symbol": best_match['symbol_code'],
                    "exchange_code": best_match['exchange_code'],
                    "type": best_match['type'],
                    "name": best_match['name'],
                    "confidence": 1.0,
                    "suggestions": []
                }, indent=2)
            )]

        # No exact match - try fuzzy matching

        # Fuzzy match on symbol codes
        symbol_codes = all_symbols['symbol_code'].tolist()
        symbol_fuzzy_matches = fuzzy_match_symbols(query, symbol_codes, cutoff=0.5)

        # Fuzzy match on company names
        company_names = all_symbols['name'].tolist()
        name_fuzzy_matches = fuzzy_match_symbols(query, company_names, cutoff=0.5)

        # Combine matches
        all_fuzzy_matches = []

        for match_text, confidence in symbol_fuzzy_matches:
            match_row = all_symbols[all_symbols['symbol_code'] == match_text].iloc[0]
            all_fuzzy_matches.append({
                'symbol_code': match_row['symbol_code'],
                'exchange_code': match_row['exchange_code'],
                'type': match_row['type'],
                'name': match_row['name'],
                'confidence': confidence,
                'match_type': 'symbol'
            })

        for match_text, confidence in name_fuzzy_matches:
            match_row = all_symbols[all_symbols['name'] == match_text].iloc[0]
            all_fuzzy_matches.append({
                'symbol_code': match_row['symbol_code'],
                'exchange_code': match_row['exchange_code'],
                'type': match_row['type'],
                'name': match_row['name'],
                'confidence': confidence,
                'match_type': 'name'
            })

        if not all_fuzzy_matches:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "valid": False,
                    "error": f"No matches found for '{query}'",
                    "confidence": 0.0,
                    "suggestions": []
                }, indent=2)
            )]

        # Sort by confidence, then prioritize by exchange/type
        all_fuzzy_matches = sorted(all_fuzzy_matches, key=lambda x: x['confidence'], reverse=True)
        all_fuzzy_matches = prioritize_matches(all_fuzzy_matches, prefer_us=not is_swedish_query)

        # Best match
        best_match = all_fuzzy_matches[0]

        # Prepare suggestions (top 5)
        suggestions = [
            {
                "symbol_code": m['symbol_code'],
                "exchange_code": m['exchange_code'],
                "type": m['type'],
                "name": m['name'],
                "confidence": m['confidence']
            }
            for m in all_fuzzy_matches[:5]
        ]

        return [TextContent(
            type="text",
            text=json.dumps({
                "valid": best_match['confidence'] >= 0.5,
                "matched_symbol": best_match['symbol_code'],
                "exchange_code": best_match['exchange_code'],
                "type": best_match['type'],
                "name": best_match['name'],
                "confidence": best_match['confidence'],
                "suggestions": suggestions if best_match['confidence'] < 1.0 else []
            }, indent=2)
        )]

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
        # Optimized query using JOIN instead of IN subquery (72x faster with v1.23 indexes)
        update_status_sql = '''
        WITH LatestUpdates AS (
            SELECT
                ed.exchange_code,
                MAX(ed.date) AS latest_date
            FROM eod_data ed
            INNER JOIN subscribed_symbols ss
                ON ed.symbol_code = ss.symbol_code
                AND ed.exchange_code = ss.exchange_code
                AND ss.is_subscribed = 1
            GROUP BY ed.exchange_code
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
        # Validate against the exchange_symbols master table — subscribed_symbols
        # only contains already-subscribed symbols, so it cannot vouch for the
        # existence of a NEW symbol the caller is asking us to subscribe to.
        master = db.sql(
            f"SELECT 1 FROM exchange_symbols "
            f"WHERE code = '{symbol_code}' "
            f"AND exchange_code = '{exchange_code}' "
            f"AND type = '{symbol_type}' LIMIT 1"
        )
        if master.empty:
            return [TextContent(
                type="text",
                text=f"Symbol not found in exchange_symbols: {exchange_code}/{symbol_type}/{symbol_code}\n"
                     f"If you expect this symbol to exist, run update_symbol_universe.sh "
                     f"on Mac mini to refresh the master symbol list."
            )]

        existing = db.get_subscribed_symbols(exchange_code=exchange_code, type=symbol_type)
        if not existing[existing['symbol_code'] == symbol_code].empty:
            return [TextContent(
                type="text",
                text=f"Already subscribed: {exchange_code}/{symbol_type}/{symbol_code}"
            )]

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
    import json

    # Extract return_json parameter before validation
    return_json = arguments.get("return_json", False)

    try:
        # === VALIDATION ERRORS ===
        exchange_code = arguments.get("exchange_code", "").upper()
        symbol_code = arguments.get("symbol_code", "").upper()
        symbol_type = arguments.get("type", "")

        if not exchange_code or not symbol_code or not symbol_type:
            error_response = {
                "status": "error",
                "error_type": "validation",
                "message": "Missing required parameters: exchange_code, symbol_code, and type are all required",
                "symbol": f"{exchange_code}_{symbol_type.replace(' ', '_')}_{symbol_code}" if exchange_code and symbol_code else None,
                "data": None
            }
            return [TextContent(type="text", text=json.dumps(error_response))]

        # Check if API key is configured
        if not EOD_API_KEY:
            error_response = {
                "status": "error",
                "error_type": "validation",
                "message": "EOD_API_KEY environment variable is not set. Intraday quotes require a valid API key.",
                "symbol": f"{exchange_code}_{symbol_type.replace(' ', '_')}_{symbol_code}",
                "data": None
            }
            return [TextContent(type="text", text=json.dumps(error_response))]

        db = get_database()
        try:
            # === DATA ERRORS ===
            # Verify the symbol exists with the correct type
            df_symbols = db.get_subscribed_symbols(exchange_code=exchange_code)
            symbol_row = df_symbols[
                (df_symbols['symbol_code'] == symbol_code) &
                (df_symbols['type'] == symbol_type)
            ]

            if symbol_row.empty:
                error_response = {
                    "status": "error",
                    "error_type": "data",
                    "message": f"Symbol not found in database: {exchange_code}/{symbol_type}/{symbol_code}",
                    "symbol": f"{exchange_code}_{symbol_type.replace(' ', '_')}_{symbol_code}",
                    "data": None
                }
                return [TextContent(type="text", text=json.dumps(error_response))]

            # Get intraday quote using the Database class method
            df = db.get_intraday_price(symbol_code, exchange_code)

            if df is None or df.empty:
                error_response = {
                    "status": "error",
                    "error_type": "data",
                    "message": f"No intraday data available for {exchange_code}/{symbol_type}/{symbol_code}",
                    "symbol": f"{exchange_code}_{symbol_type.replace(' ', '_')}_{symbol_code}",
                    "data": None
                }
                return [TextContent(type="text", text=json.dumps(error_response))]

            # === SUCCESS - Build response ===
            symbol_name = symbol_row['name'].iloc[0] if symbol_row['name'].iloc[0] else symbol_code

            # Convert DataFrame to dict for JSON response
            quote_data = {}
            for idx, row in df.iterrows():
                field_name = str(idx)
                quote_data[field_name] = row['Value']

            # Build structured JSON response
            result = {
                "status": "success",
                "symbol": f"{exchange_code}_{symbol_type.replace(' ', '_')}_{symbol_code}",
                "name": symbol_name,
                "data": quote_data,
                "metadata": {
                    "exchange": exchange_code,
                    "type": symbol_type,
                    "delay_notice": "Data is delayed 15-20 minutes",
                    "source": "EOD Historical Data API"
                }
            }

            # Return based on mode
            if return_json:
                # n8n mode: return only JSON
                return [TextContent(type="text", text=json.dumps(result))]
            else:
                # Interactive mode: return formatted message + JSON
                lines = [
                    f"Intraday Quote: {symbol_code}.{exchange_code}",
                    f"Name: {symbol_name}",
                    "=" * 40,
                ]

                for field_name, value in quote_data.items():
                    display_name = field_name.replace('_', ' ').title()
                    lines.append(f"{display_name}: {value}")

                lines.append("")
                lines.append("Note: Data is delayed 15-20 minutes")
                lines.append("")
                lines.append("JSON Response:")
                lines.append(json.dumps(result, indent=2))

                return [TextContent(type="text", text="\n".join(lines))]

        finally:
            db.close()

    except ValueError as e:
        # Validation errors we didn't catch above
        error_response = {
            "status": "error",
            "error_type": "validation",
            "message": str(e),
            "symbol": f"{arguments.get('exchange_code', '')}_{arguments.get('type', '').replace(' ', '_')}_{arguments.get('symbol_code', '')}",
            "data": None
        }
        return [TextContent(type="text", text=json.dumps(error_response))]

    except FileNotFoundError as e:
        # Data availability errors
        error_response = {
            "status": "error",
            "error_type": "data",
            "message": f"Database or data not found: {str(e)}",
            "symbol": f"{arguments.get('exchange_code', '')}_{arguments.get('type', '').replace(' ', '_')}_{arguments.get('symbol_code', '')}",
            "data": None
        }
        return [TextContent(type="text", text=json.dumps(error_response))]

    except Exception as e:
        # System errors
        import traceback
        error_response = {
            "status": "error",
            "error_type": "system",
            "message": f"Unexpected error fetching intraday quote: {str(e)}",
            "symbol": f"{arguments.get('exchange_code', '')}_{arguments.get('type', '').replace(' ', '_')}_{arguments.get('symbol_code', '')}",
            "data": None,
            "details": traceback.format_exc() if not return_json else None
        }
        return [TextContent(type="text", text=json.dumps(error_response))]


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
