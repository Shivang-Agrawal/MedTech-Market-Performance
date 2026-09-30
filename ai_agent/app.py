"""
MedTech Market Performance -- AI Agent (Streamlit UI)

Free to run: bring your own API key (Anthropic or Groq -- Groq's free tier needs no payment
method). Nothing here costs the repo owner anything to host on Streamlit Community Cloud or
Hugging Face Spaces, since every visitor supplies their own key, used only for their own session.

Run locally:   streamlit run app.py
"""
import os
import streamlit as st
from agent import run_agent

st.set_page_config(page_title="MedTech Market Performance Agent", page_icon="\U0001F4CA", layout="wide")

with st.sidebar:
    st.title("\U0001F4CA MedTech Agent")
    st.caption("Ask about market share, top brands, contribution to share change, or data quality -- "
               "grounded entirely in the governed Gold dataset from this project's Power BI / Databricks build.")

    provider = st.selectbox("Provider", ["groq", "anthropic"],
                             help="Groq's free tier needs no payment method -- recommended for a quick public demo.")
    default_model = "llama-3.1-8b-instant" if provider == "groq" else "claude-sonnet-4-5"
    model = st.text_input("Model", value=default_model)

    env_key = os.environ.get("GROQ_API_KEY" if provider == "groq" else "ANTHROPIC_API_KEY", "")
    api_key = st.text_input(f"{provider.title()} API key", value=env_key, type="password",
                             help="Stored only in this browser session, never written to disk or committed. "
                                  "Get a free Groq key at console.groq.com/keys, or an Anthropic key at console.anthropic.com.")

    st.divider()
    show_trace = st.checkbox("Show tool calls (validation trace)", value=True,
                              help="Every number the agent reports comes from a named tool call against "
                                   "the governed dataset -- this shows exactly which one, and its raw output.")
    if st.button("Clear conversation"):
        st.session_state.pop("messages", None)
        st.rerun()

    st.divider()
    st.caption("This agent implements the AI Prompting Pack's structured prompt (data source, metric, "
               "period, comparison rule, missing-data rules, output format, and a prohibition on "
               "unsupported causal claims) as its system prompt -- see prompts/system_prompt.md.")

st.title("MedTech Market Performance -- AI Agent")
st.caption("Sample questions: *\"Why did market share change year over year?\"* \u00b7 "
           "*\"Which products contributed most to the share change?\"* \u00b7 "
           "*\"What are the top 10 brands this year?\"* \u00b7 *\"Can these numbers be trusted?\"*")

if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])
        if m["role"] == "assistant" and m.get("trace") and show_trace:
            with st.expander(f"\U0001F50D {len(m['trace'])} tool call(s) behind this answer"):
                for t in m["trace"]:
                    st.markdown(f"**`{t['tool']}`**  args: `{t['args']}`")
                    st.json(t["result"])

question = st.chat_input("Ask a question about market share, contribution, brands, or data quality...")

if question:
    if not api_key:
        st.error(f"Enter a {provider} API key in the sidebar first (it's free to get one).")
        st.stop()

    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("Querying the governed dataset and reasoning..."):
            result = run_agent(st.session_state.messages, provider, api_key, model)
        st.markdown(result["reply"])
        if result["trace"] and show_trace:
            with st.expander(f"\U0001F50D {len(result['trace'])} tool call(s) behind this answer"):
                for t in result["trace"]:
                    st.markdown(f"**`{t['tool']}`**  args: `{t['args']}`")
                    st.json(t["result"])

    st.session_state.messages.append({"role": "assistant", "content": result["reply"], "trace": result["trace"]})
