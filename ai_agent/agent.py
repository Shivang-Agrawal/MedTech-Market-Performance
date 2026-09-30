"""
Provider-agnostic tool-calling agent loop.

Supports two backends, both chosen so this project costs $0 to run for anyone who clones the
repo and gets their own free API key:

  - "anthropic"  -> Claude models via the official `anthropic` SDK (person supplies their own
                    Anthropic API key; Anthropic offers free trial credits for new accounts).
  - "groq"       -> Free, fast inference for open-weight models (Llama 3.1, etc.) via Groq's
                    OpenAI-compatible API. Groq's free tier is the recommended default for a
                    public demo since it requires no payment method at signup.

Neither backend is hardcoded with a key -- the person using the app supplies their own via the
Streamlit sidebar (see app.py) or an environment variable. This repo never stores or transmits
a key anywhere except directly to the chosen provider's official API.
"""
from __future__ import annotations
import json
import os
from tools.data_tools import TOOL_FUNCTIONS
from tools.schema import as_anthropic_tools, as_openai_tools

SYSTEM_PROMPT = open(os.path.join(os.path.dirname(__file__), "prompts", "system_prompt.md")).read()

MAX_TOOL_ROUNDS = 6  # hard cap so a misbehaving model can't loop forever burning API calls


def _execute_tool(name: str, args: dict) -> dict:
    fn = TOOL_FUNCTIONS.get(name)
    if fn is None:
        return {"error": f"Unknown tool '{name}'"}
    try:
        return fn(**args)
    except Exception as e:  # a tool error becomes visible data for the model, not a crash
        return {"error": f"{type(e).__name__}: {e}"}


def run_agent_anthropic(history: list[dict], api_key: str, model: str = "claude-sonnet-4-5") -> dict:
    """history: list of {'role': 'user'|'assistant', 'content': str}. Returns
    {'reply': str, 'trace': [ {tool, args, result}, ... ]} so the UI can show exactly which
    governed data the answer came from -- the same transparency principle as the validation
    checklist in the AI Prompting Pack."""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    messages = [{"role": m["role"], "content": m["content"]} for m in history]
    trace = []

    for _ in range(MAX_TOOL_ROUNDS):
        resp = client.messages.create(
            model=model, max_tokens=1500, system=SYSTEM_PROMPT,
            tools=as_anthropic_tools(), messages=messages,
        )
        if resp.stop_reason != "tool_use":
            text = "".join(b.text for b in resp.content if b.type == "text")
            return {"reply": text, "trace": trace}

        messages.append({"role": "assistant", "content": resp.content})
        tool_results = []
        for block in resp.content:
            if block.type != "tool_use":
                continue
            result = _execute_tool(block.name, block.input)
            trace.append({"tool": block.name, "args": block.input, "result": result})
            tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": json.dumps(result, default=str)})
        messages.append({"role": "user", "content": tool_results})

    return {"reply": "(stopped after maximum tool-call rounds -- the question may need to be narrower)", "trace": trace}


def run_agent_groq(history: list[dict], api_key: str, model: str = "llama-3.1-8b-instant") -> dict:
    """Same contract as run_agent_anthropic, using Groq's OpenAI-compatible chat.completions API."""
    from openai import OpenAI
    client = OpenAI(api_key=api_key, base_url="https://api.groq.com/openai/v1")
    messages = [{"role": "system", "content": SYSTEM_PROMPT}] + [{"role": m["role"], "content": m["content"]} for m in history]
    trace = []

    for _ in range(MAX_TOOL_ROUNDS):
        resp = client.chat.completions.create(model=model, messages=messages, tools=as_openai_tools(), max_tokens=1500)
        msg = resp.choices[0].message
        if not msg.tool_calls:
            return {"reply": msg.content or "", "trace": trace}

        messages.append({"role": "assistant", "content": msg.content, "tool_calls": [tc.model_dump() for tc in msg.tool_calls]})
        for tc in msg.tool_calls:
            args = json.loads(tc.function.arguments or "{}")
            result = _execute_tool(tc.function.name, args)
            trace.append({"tool": tc.function.name, "args": args, "result": result})
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": json.dumps(result, default=str)})

    return {"reply": "(stopped after maximum tool-call rounds -- the question may need to be narrower)", "trace": trace}


def run_agent(history: list[dict], provider: str, api_key: str, model: str) -> dict:
    if provider == "anthropic":
        return run_agent_anthropic(history, api_key, model)
    elif provider == "groq":
        return run_agent_groq(history, api_key, model)
    raise ValueError(f"Unknown provider: {provider}")
