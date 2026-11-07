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
    CallToolResult,
    ListToolsResult,
    ListResourcesResult,
    ReadResourceResult,
    TextResourceContents,
)

from marketdata.database import Database


# Environment variables
MARKETDATA_DB_PATH = os.environ.get("MARKETDATA_DB_PATH", "")
EOD_API_KEY = os.environ.get("EOD_API_KEY", "")


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
            name="list_macro_series",
            description="List the macro series stored in the database: FRED/ALFRED (GDP, CPI, payrolls, yields, ...) and SqueezeMetrics (SQZ_GEX: dealer gamma exposure of SPX options in $, SQZ_DIX: Dark Index; daily since 2011): id, title, frequency, units, seasonal adjustment, whether full revision history (vintages) is kept, observation date range, earliest stored vintage, and the source's last update.",
            inputSchema={"type": "object", "properties": {}}
        ),
        Tool(
            name="get_macro_series",
            description="Get a macro series by id: FRED ids (e.g. GDPC1, CPIAUCSL, PAYEMS, DGS10) or SqueezeMetrics SQZ_GEX (SPX dealer gamma, negative = dealers short gamma, i.e. volatility-amplifying) and SQZ_DIX (Dark Index). Default returns today's (revised) values. Pass as_of='YYYY-MM-DD' for point-in-time values exactly as published on that day (ALFRED vintages; returns nothing for dates before the series' earliest stored vintage), or release='first' for each observation's first-published value. Use list_macro_series to discover ids.",
            inputSchema={
                "type": "object",
                "properties": {
                    "series_id": {
                        "type": "string",
                        "description": "Series id, e.g. GDPC1 or SQZ_GEX"
                    },
                    "as_of": {
                        "type": "string",
                        "description": "Optional point-in-time date YYYY-MM-DD: values as published on that day"
                    },
                    "release": {
                        "type": "string",
                        "enum": ["current", "first"],
                        "description": "current (default) = latest revised values; first = first-published value per observation. Ignored when as_of is given.",
                        "default": "current"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Optional first observation date YYYY-MM-DD"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "Optional last observation date YYYY-MM-DD"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of rows to return (default 60, most recent first)",
                        "default": 60
                    }
                },
                "required": ["series_id"]
            }
        ),
        Tool(
            name="get_macro_revisions",
            description="Show the full revision history of one observation of a FRED macro series, e.g. how Q1-2020 real GDP (GDPC1, 2020-01-01) was published and then revised over time. Only meaningful for series stored with vintages (see list_macro_series).",
            inputSchema={
                "type": "object",
                "properties": {
                    "series_id": {
                        "type": "string",
                        "description": "Series id, e.g. GDPC1 or SQZ_GEX"
                    },
                    "observation_date": {
                        "type": "string",
                        "description": "Observation date YYYY-MM-DD as FRED dates it (period start: Q2 = YYYY-04-01, a month = YYYY-MM-01)"
                    }
                },
                "required": ["series_id", "observation_date"]
            }
        ),
        Tool(
            name="list_cot_markets",
            description="List the CFTC Commitments of Traders (COT) markets stored in the database: CFTC market code, name, exchange, commodity group, the report types available (legacy_fut, legacy_comb, disagg_fut, disagg_comb, tff_fut, tff_comb, cit), first/last report date and the mapped EOD symbol (e.g. GSPC for S&P 500 futures positioning) where one exists. Use it to find the market code for get_cot. Needs database schema 1.27+.",
            inputSchema={
                "type": "object",
                "properties": {
                    "search": {
                        "type": "string",
                        "description": "Optional case-insensitive text matched against market name, commodity, CFTC code and EOD symbol (e.g. 'crude', 'gold', 'S&P')"
                    },
                    "report_type": {
                        "type": "string",
                        "enum": ["legacy_fut", "legacy_comb", "disagg_fut", "disagg_comb", "tff_fut", "tff_comb", "cit"],
                        "description": "Optional: only markets that have reports of this type"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of markets to return (default 50)",
                        "default": 50
                    }
                }
            }
        ),
        Tool(
            name="get_cot",
            description="Get CFTC Commitments of Traders positioning for one market: per trader category long, short and net (long - short) contracts plus open interest, most recent report first. IMPORTANT timing: report_date is the Tuesday the positions were measured on (snapshot); released is the Friday the CFTC published them (shifted for holidays). The data is only knowable from `released`, so backtests and signals must key on released, never on report_date. Pass as_of='YYYY-MM-DD' for the point-in-time view: only reports released on or before that day (e.g. as_of 2026-11-05 ends at the 2026-10-27 report). Report types: tff_fut (financial futures: dealer, asset_mgr, lev_money, other_rept, nonrept), disagg_fut (commodities: prod_merc, swap, money_manager, other_rept, nonrept), legacy_fut (commercial / noncommercial / nonreportable, longest history), cit (index traders); *_comb = futures and options combined. Market is a CFTC code (e.g. 067651) or a mapped EOD symbol (e.g. GSPC); see list_cot_markets. include_index appends a Williams-style COT index (0-100: where the category's net sits in its trailing range) per category. Needs database schema 1.27+.",
            inputSchema={
                "type": "object",
                "properties": {
                    "market": {
                        "type": "string",
                        "description": "CFTC market code (e.g. 067651) or mapped EOD symbol (e.g. GSPC)"
                    },
                    "report_type": {
                        "type": "string",
                        "enum": ["legacy_fut", "legacy_comb", "disagg_fut", "disagg_comb", "tff_fut", "tff_comb", "cit"],
                        "description": "Report type (default tff_fut)",
                        "default": "tff_fut"
                    },
                    "as_of": {
                        "type": "string",
                        "description": "Optional point-in-time date YYYY-MM-DD: only reports released (published) on or before this day"
                    },
                    "weeks": {
                        "type": "integer",
                        "description": "Number of most recent reports to return (default 52)",
                        "default": 52
                    },
                    "categories": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of trader categories to show (e.g. ['lev_money', 'asset_mgr']); default all"
                    },
                    "include_index": {
                        "type": "boolean",
                        "description": "Append a COT index column (0-100) for each category's net position (default false)",
                        "default": False
                    },
                    "lookback_weeks": {
                        "type": "integer",
                        "description": "Window of the COT index in weeks (default 156 = 3 years); only used with include_index",
                        "default": 156
                    }
                },
                "required": ["market"]
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
        elif name == "list_macro_series":
            return await handle_list_macro_series(arguments)
        elif name == "get_macro_series":
            return await handle_get_macro_series(arguments)
        elif name == "get_macro_revisions":
            return await handle_get_macro_revisions(arguments)
        elif name == "list_cot_markets":
            return await handle_list_cot_markets(arguments)
        elif name == "get_cot":
            return await handle_get_cot(arguments)
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
    """Subscribe to a symbol AND immediately fetch its full historical EOD data.

    Uses Database.subscribe_and_fetch_symbol() which:
      1. Validates the symbol exists in exchange_symbols
      2. Inserts (or updates) the subscribed_symbols row with is_subscribed=1
      3. Calls the EOD provider API to fetch the symbol's full price history
      4. Appends the rows to eod_data (UNIQUE constraint on (symbol,exchange,date)
         deduplicates if the symbol was already partially populated)

    Idempotent: safe to call on already-subscribed symbols — re-fetches any
    missing history without creating duplicates.
    """
    exchange_code = arguments["exchange_code"].upper()
    symbol_code = arguments["symbol_code"].upper()
    symbol_type = arguments["type"]

    db = get_database()
    try:
        # Check current subscription + data state so we can give a clear report
        existing_subs = db.get_subscribed_symbols(exchange_code=exchange_code, type=symbol_type)
        already_subscribed = not existing_subs[existing_subs['symbol_code'] == symbol_code].empty

        existing_rows = db.sql(
            f"SELECT COUNT(*) AS n FROM eod_data "
            f"WHERE symbol_code = '{symbol_code}' "
            f"AND exchange_code = '{exchange_code}'"
        ).iloc[0]['n']

        # If fully populated already, skip the API round-trip
        if already_subscribed and existing_rows > 0:
            return [TextContent(
                type="text",
                text=f"Already subscribed with data: "
                     f"{exchange_code}/{symbol_type}/{symbol_code} "
                     f"({existing_rows} EOD rows)\n"
                     f"To force a re-fetch, unsubscribe and re-subscribe."
            )]

        # Either new subscription OR subscribed-but-empty — fetch full history.
        # subscribe_and_fetch_symbol raises ValueError if the symbol isn't in
        # exchange_symbols, which we relay to the caller.
        try:
            num_rows = db.subscribe_and_fetch_symbol(
                symbol_code=symbol_code,
                exchange_code=exchange_code,
            )
        except ValueError as e:
            return [TextContent(
                type="text",
                text=f"Symbol not found in exchange_symbols: "
                     f"{exchange_code}/{symbol_type}/{symbol_code}\n"
                     f"If you expect this symbol to exist, run "
                     f"update_symbol_universe.sh on Mac mini to refresh the "
                     f"master symbol list.\n\nDetails: {e}"
            )]

        verb = "Re-fetched history for previously empty subscription" \
            if already_subscribed else "Subscribed and fetched history"
        return [TextContent(
            type="text",
            text=f"{verb}: {exchange_code}/{symbol_type}/{symbol_code}\n"
                 f"{num_rows} EOD rows inserted from the provider API."
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


def _format_macro_value(value) -> str:
    """Format a macro observation value; NULL/NaN (FRED '.') shows as n/a."""
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:,.4f}".rstrip("0").rstrip(".")


def _macro_series_meta(db: Database, series_id: str) -> Optional[dict]:
    """Return the macro_series row for series_id as a dict, or None."""
    df = db.get_macro_series_info(subscribed_only=False)
    row = df[df['series_id'] == series_id]
    if row.empty:
        return None
    return row.iloc[0].to_dict()


async def handle_list_macro_series(arguments: dict[str, Any]) -> list[TextContent]:
    """List stored macro series (FRED/ALFRED and SqueezeMetrics) with coverage."""
    db = get_database()
    try:
        df_info = db.get_macro_series_info(subscribed_only=False)
        if df_info.empty:
            return [TextContent(type="text", text="No macro series in the database.")]

        df_cov = db.sql(
            "SELECT series_id, MIN(date) AS first_date, MAX(date) AS last_date, "
            "MIN(realtime_start) AS first_vintage "
            "FROM macro_observations GROUP BY series_id")
        df = df_info.merge(df_cov, on='series_id', how='left')

        lines = [f"Macro series ({len(df)}):", ""]
        lines.append("Series | Freq | Vintages | Observations | Earliest vintage | Units | Title")
        lines.append("-" * 100)
        for _, r in df.iterrows():
            vint = "yes" if r['keep_vintages'] else "latest only"
            sub = "" if r['is_subscribed'] else " (unsubscribed)"
            sa = f", {r['seasonal_adjustment']}" if r['seasonal_adjustment'] else ""
            lines.append(
                f"{r['series_id']}{sub} | {r['frequency']} | {vint} | "
                f"{r['first_date']} .. {r['last_date']} | {r['first_vintage']} | "
                f"{r['units']}{sa} | {r['title']}")
        lines.append("")
        lines.append("Point-in-time (as_of) queries cannot reach before a series' earliest vintage.")
        return [TextContent(type="text", text="\n".join(lines))]
    finally:
        db.close()


async def handle_get_macro_series(arguments: dict[str, Any]) -> list[TextContent]:
    """Get a macro series: current, first-release, or point-in-time values."""
    series_id = arguments["series_id"].upper()
    as_of = arguments.get("as_of")
    release = arguments.get("release", "current")
    start_date = arguments.get("start_date")
    end_date = arguments.get("end_date")
    limit = arguments.get("limit", 60)

    db = get_database()
    try:
        meta = _macro_series_meta(db, series_id)
        if meta is None:
            return [TextContent(
                type="text",
                text=f"Macro series not found: {series_id} (see list_macro_series)")]

        if as_of:
            df = db.get_macro_series(series_id, as_of=as_of,
                                     start=start_date, end=end_date)
            mode = f"as published on {as_of}"
        elif release == "first":
            df = db.get_macro_first_release(series_id, start=start_date,
                                            end=end_date)
            mode = "first release"
        else:
            df = db.get_macro_series(series_id, start=start_date, end=end_date)
            mode = "current (latest revision)"

        if df is None or df.empty:
            hint = ""
            if as_of:
                hint = (" -- as_of may be before the series' earliest stored "
                        "vintage (see list_macro_series)")
            return [TextContent(
                type="text", text=f"No data for {series_id}, {mode}{hint}")]

        total = len(df)
        df = df.sort_index(ascending=False).head(limit)

        sa = f", {meta['seasonal_adjustment']}" if meta['seasonal_adjustment'] else ""
        lines = [
            f"{series_id} -- {meta['title']}",
            f"Units: {meta['units']}{sa} | Frequency: {meta['frequency']} | {mode}",
            f"{len(df)} of {total} rows, most recent first:",
            "",
            "Date       | Value",
            "-" * 30,
        ]
        for date, row in df.iterrows():
            lines.append(f"{date.strftime('%Y-%m-%d')} | {_format_macro_value(row['value'])}")

        return [TextContent(type="text", text="\n".join(lines))]
    finally:
        db.close()


async def handle_get_macro_revisions(arguments: dict[str, Any]) -> list[TextContent]:
    """Show every published vintage of one macro observation."""
    series_id = arguments["series_id"].upper()
    observation_date = arguments["observation_date"]

    db = get_database()
    try:
        meta = _macro_series_meta(db, series_id)
        if meta is None:
            return [TextContent(
                type="text",
                text=f"Macro series not found: {series_id} (see list_macro_series)")]

        df = db.get_macro_vintages(series_id)
        df = df[df['date'] == observation_date]
        if df.empty:
            return [TextContent(
                type="text",
                text=f"No observation {observation_date} for {series_id} "
                     f"(FRED dates periods by their start, e.g. Q2 = YYYY-04-01)")]

        lines = [
            f"{series_id} {observation_date} -- {meta['title']} ({meta['units']})",
            f"{len(df)} published version(s):",
            "",
            "Published from | Valid until  | Value",
            "-" * 45,
        ]
        for _, r in df.iterrows():
            until = "current" if r['realtime_end'] == '9999-12-31' else r['realtime_end']
            lines.append(f"{r['realtime_start']}     | {until:<12} | {_format_macro_value(r['value'])}")
        if not meta['keep_vintages']:
            lines.append("")
            lines.append("Note: this series is stored latest-only; no revision history is kept.")

        return [TextContent(type="text", text="\n".join(lines))]
    finally:
        db.close()

COT_SCHEMA_MSG = ("CFTC COT tables not found: the database is older than schema "
                  "1.27 (COT needs marketdata-db 1.13.0 / schema 1.27; run the "
                  "install/migrate_to_v127.py migration).")


def _cot_available(db: Database) -> bool:
    """True when the database has the COT tables (schema 1.27+)."""
    df = db.sql("SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('cot_markets', 'cot_reports', 'cot_positions')")
    return len(df) == 3


def _format_cot_int(value) -> str:
    """Format a contract count; NULL/NaN shows as n/a."""
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:,.0f}"


async def handle_list_cot_markets(arguments: dict[str, Any]) -> list[TextContent]:
    """List CFTC COT markets with report types, coverage and EOD mapping."""
    search = arguments.get("search")
    report_type = arguments.get("report_type")
    limit = arguments.get("limit", 50)

    db = get_database()
    try:
        if not _cot_available(db):
            return [TextContent(type="text", text=COT_SCHEMA_MSG)]
        df = db.get_cot_markets(search=search, report_type=report_type)
        if df.empty:
            return [TextContent(type="text", text="No COT markets found.")]

        total = len(df)
        df = df.head(limit)
        lines = [
            f"COT markets ({len(df)} of {total}):",
            "",
            "Code | Name | Exchange | Group | Report types | First .. Last report | EOD symbol",
            "-" * 100,
        ]
        for _, r in df.iterrows():
            eod = ""
            if r['eod_symbol']:
                eod = f"{r['eod_symbol']}.{r['eod_exchange']}" if r['eod_exchange'] else r['eod_symbol']
            group = r['commodity_group'] or ""
            if r['commodity_subgroup']:
                group = f"{group}/{r['commodity_subgroup']}"
            lines.append(
                f"{r['market_code']} | {r['market_and_exchange_name']} | "
                f"{r['exchange_code'] or ''} | {group} | {r['report_types'] or ''} | "
                f"{r['first_report_date']} .. {r['last_report_date']} | {eod}")
        if total > len(df):
            lines.append("")
            lines.append(f"{total - len(df)} more; narrow with search/report_type or raise limit.")
        return [TextContent(type="text", text="\n".join(lines))]
    except ValueError as e:
        return [TextContent(type="text", text=f"Error: {e}")]
    finally:
        db.close()


async def handle_get_cot(arguments: dict[str, Any]) -> list[TextContent]:
    """Get COT positions of one market, optionally point in time, with COT index."""
    market = arguments["market"].strip()
    report_type = arguments.get("report_type", "tff_fut")
    as_of = arguments.get("as_of")
    weeks = arguments.get("weeks", 52)
    categories = arguments.get("categories")
    include_index = arguments.get("include_index", False)
    lookback_weeks = arguments.get("lookback_weeks", 156)

    db = get_database()
    try:
        if not _cot_available(db):
            return [TextContent(type="text", text=COT_SCHEMA_MSG)]

        try:
            df = db.get_cot(market, report_type=report_type, as_of=as_of)
        except ValueError as e:
            return [TextContent(
                type="text", text=f"Error: {e} (see list_cot_markets)")]

        mode = f"as published by {as_of}" if as_of else "all releases to date"
        if df.empty:
            return [TextContent(
                type="text",
                text=f"No COT data for {market}, {report_type}, {mode} "
                     f"(check the report type with list_cot_markets)")]

        have = [c[:-4] for c in df.columns if c.endswith('_net')]
        if categories:
            wanted = [c.lower() for c in categories]
            bad = [c for c in wanted if c not in have]
            if bad:
                return [TextContent(
                    type="text",
                    text=f"Error: unknown categories {bad} for {report_type} (have {have})")]
            have = wanted

        index_cols = {}
        if include_index:
            for cat in have:
                index_cols[cat] = db.get_cot_index(
                    market, cat, report_type=report_type,
                    lookback_weeks=lookback_weeks, as_of=as_of)

        total = len(df)
        df = df.sort_index(ascending=False).head(weeks)

        cols = []
        for cat in have:
            cols += [f"{cat}_long", f"{cat}_short", f"{cat}_net"]
        header = ["report_date", "released", "open_interest"] + cols
        if include_index:
            header += [f"{cat}_index" for cat in have]
        lines = [
            f"COT {market} -- {report_type} | {mode}",
            f"{len(df)} of {total} reports, most recent first. report_date = Tuesday "
            f"snapshot, released = publication date (key backtests on released).",
            "",
            " | ".join(header),
            "-" * 100,
        ]
        for date, row in df.iterrows():
            vals = [date.strftime('%Y-%m-%d'), str(row['released']),
                    _format_cot_int(row['open_interest'])]
            vals += [_format_cot_int(row[c]) for c in cols]
            for cat in have if include_index else []:
                v = index_cols[cat].get(date)
                vals.append("n/a" if v is None or pd.isna(v) else f"{v:.0f}")
            lines.append(" | ".join(vals))
        if include_index:
            lines.append("")
            lines.append(f"index = 0-100 position of net within the trailing "
                         f"{lookback_weeks} reports (n/a until the window is full).")
        return [TextContent(type="text", text="\n".join(lines))]
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


# --- MCP SDK 2.x adapters ----------------------------------------------------
# list_tools()/call_tool()/list_resources()/read_resource() above keep their
# v1 signatures so tests and scripts can import and call them directly. These
# thin adapters bridge them to the SDK 2.x handler contract (ctx/params in,
# *Result models out) and restore v1 error semantics: any exception from the
# legacy tool handler becomes CallToolResult(is_error=True, text=str(e)) —
# readable by the model — instead of an opaque JSON-RPC internal error.

async def _on_list_tools(ctx, params) -> ListToolsResult:
    return ListToolsResult(tools=await list_tools())


async def _on_call_tool(ctx, params) -> CallToolResult:
    try:
        content = await call_tool(params.name, params.arguments or {})
        return CallToolResult(content=list(content), is_error=False)
    except Exception as e:
        return CallToolResult(
            content=[TextContent(type="text", text=str(e))],
            is_error=True,
        )


async def _on_list_resources(ctx, params) -> ListResourcesResult:
    return ListResourcesResult(resources=await list_resources())


async def _on_read_resource(ctx, params) -> ReadResourceResult:
    # v1 auto-wrapped a returned str as text/plain contents; reproduce that.
    text = await read_resource(params.uri)
    return ReadResourceResult(
        contents=[
            TextResourceContents(uri=params.uri, mime_type="text/plain", text=text)
        ]
    )


server = Server(
    "marketdata-db",
    version="1.0.0",
    on_list_tools=_on_list_tools,
    on_call_tool=_on_call_tool,
    on_list_resources=_on_list_resources,
    on_read_resource=_on_read_resource,
)


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
