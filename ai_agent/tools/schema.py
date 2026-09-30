"""
JSON schema definitions for each tool in data_tools.py, written once and translated into
whichever shape the active LLM provider expects (Anthropic's `tools` format or the
OpenAI-compatible `tools` format used by Groq/OpenAI). Keeping one source of truth means the
agent behaves identically regardless of which free-tier provider a person plugs in.
"""

TOOL_SPECS = [
    {
        "name": "get_market_share_summary",
        "description": "Market Sales, Company Sales, Market Share % and YoY change for a range of "
                        "months in a given year, using only like-for-like complete months. Always "
                        "call this first for any question about overall performance or share change.",
        "parameters": {
            "type": "object",
            "properties": {
                "year": {"type": "integer", "description": "Current year, e.g. 2026", "default": 2026},
                "start_month": {"type": "integer", "description": "1-12, start of range", "default": 1},
                "end_month": {"type": "integer", "description": "1-12, end of range", "default": 7},
            },
        },
    },
    {
        "name": "get_contribution_breakdown",
        "description": "Additive share-change contribution (pp) broken down by product, brand, "
                        "category, subcategory, region, or state. Contributions sum exactly to the "
                        "total share change for the same period. Use this to answer 'which products "
                        "drove the change'.",
        "parameters": {
            "type": "object",
            "properties": {
                "year": {"type": "integer", "default": 2026},
                "start_month": {"type": "integer", "default": 1},
                "end_month": {"type": "integer", "default": 7},
                "group_by": {"type": "string", "enum": ["ProductCode", "Brand", "Category", "Subcategory", "Region", "State"],
                             "default": "ProductCode"},
                "top_n": {"type": "integer", "description": "How many top/bottom rows to return", "default": 10},
            },
        },
    },
    {
        "name": "get_top_brands",
        "description": "Top N brands ranked by Market Sales, with Company Sales and Market Share % "
                        "per brand, for a year (optionally a single month).",
        "parameters": {
            "type": "object",
            "properties": {
                "year": {"type": "integer", "default": 2026},
                "month": {"type": "integer", "description": "1-12, optional single month"},
                "n": {"type": "integer", "default": 10},
            },
        },
    },
    {
        "name": "get_period_completeness",
        "description": "Which months are flagged Complete vs Incomplete for a year, and the latest "
                        "complete month. ALWAYS call this before trusting any YoY figure that touches "
                        "the most recent month(s) in the dataset.",
        "parameters": {
            "type": "object",
            "properties": {"year": {"type": "integer", "default": 2026}},
        },
    },
    {
        "name": "get_data_quality_summary",
        "description": "Row counts by status (Accepted/Duplicate/Inactive/Unmapped/Ambiguous) and "
                        "which specific product codes were excluded and why. Use this for any question "
                        "about data trustworthiness, exclusions, or rejected rows.",
        "parameters": {"type": "object", "properties": {}},
    },
    {
        "name": "get_control_reconciliation",
        "description": "Recomputes every control-total metric live and compares it to the "
                        "vendor-supplied expected value. Use this if asked whether the numbers can be "
                        "trusted or have been validated -- never assert validation without calling this.",
        "parameters": {"type": "object", "properties": {}},
    },
]


def as_anthropic_tools():
    return [{"name": t["name"], "description": t["description"], "input_schema": t["parameters"]} for t in TOOL_SPECS]


def as_openai_tools():
    return [{"type": "function", "function": {"name": t["name"], "description": t["description"], "parameters": t["parameters"]}}
            for t in TOOL_SPECS]
