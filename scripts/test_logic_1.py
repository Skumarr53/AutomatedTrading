"""
Standalone Streamlit test app for link click tracking functionality.

Run with:
    streamlit run app_click_test.py
"""

import os
import re
import sys
import hashlib
from datetime import datetime

import pandas as pd
import streamlit as st
from pdb import set_trace

# ============ INITIALIZATION ============
print("✅ Script started successfully!")
sys.stdout.flush()

SAMPLE_RESPONSE = """
Industry CoWoS capacity is expected to grow from 756,000 wafers in 2025 to 1,170,000 wafers in 2026, with TSMC maintaining the majority share alongside non-TSMC providers like Amkor, UMC, and ASE [NVIDIA Corp "A Bit Noisy, But Strong Demand Should Carry The Day; Buy" (Buy) (2025-08-28) — Link — UBS](https://google.com)

Backend Testing and Assembly:
Backend tester capacity has been a bottleneck, but lead times for Advantest testers have improved, supporting a return to normal test times [NVIDIA Corp.: Earnings Preview: Setting up for a very strong 2026 (2025-08-18) — Link — Morgan Stanley](https://example.com/morgan-stanley-report-2)

KYEC tested B200/B300 units are expected to grow from 1mn in Q2 to 1.5mn in Q3 [NVIDIA Corp.: Earnings Preview: Setting up for a very strong 2026 (2025-08-18) — Link — Morgan Stanley](https://example.com/morgan-stanley-report-3)
"""


# ============ PAGE CONFIG ============
try:
    st.set_page_config(page_title="Link Tracking Test", page_icon="🧪", layout="wide")
except Exception as e:
    print(f"⚠️ Error setting page config: {e}")


# ============ SESSION STATE ============
if "link_click_stats" not in st.session_state:
    st.session_state.link_click_stats = []

if "session_id" not in st.session_state:
    st.session_state.session_id = "test_session_123"


# ============ HELPER FUNCTIONS ============
def extract_document_title(title_date_str: str) -> str:
    """Extract document title from citation string."""
    match = re.match(r"^(.*?)\s*\(", title_date_str)
    return match.group(1).strip() if match else title_date_str.strip()


def extract_document_date(title_date_str: str) -> str | None:
    """Extract date from citation string."""
    match = re.search(r"\((\d{4}-\d{2}-\d{2})\)", title_date_str)
    return match.group(1) if match else None


class MockMessage:
    """Mock message object for testing."""

    def __init__(self, content: str, request_id: str):
        self.content = content
        self.request_id = request_id


# ============ CSS INJECTION ============
if "link_css_injected" not in st.session_state:
    st.markdown(
        """
        <style>
        /* Keep the entire line tight and inline */
        .citation-line { 
            margin: 0 !important; 
            padding: 0 !important; 
            line-height: 1.25 !important; 
        }
        .citation-inline {
            display: inline-flex !important;
            align-items: center !important;
            gap: 0.25rem !important;
        }
        .citation-frag {
            display: inline !important;
            font-size: 0.9rem !important;
        }
        
        /* Inline wrapper for the button container */
        .icon-link-wrapper {
            display: inline-flex !important;
            align-items: center !important;
            margin: 0 !important;
            padding: 0 !important;
            vertical-align: middle !important;
        }

        /* Streamlit button container - make inline */
        div[data-testid="stButton"] {
            display: inline-flex !important;
            margin: 0 !important;
            padding: 0 !important;
            vertical-align: middle !important;
        }

        /* The button itself */
        div[data-testid="stButton"] > button {
            background: transparent !important;
            border: none !important;
            padding: 0 !important;
            margin: 0 !important;
            min-width: auto !important;
            min-height: auto !important;
            width: auto !important;
            height: auto !important;
            color: #1f77b4 !important;
            font-size: 0.5rem !important;
            line-height: 0.5 !important;
            box-shadow: none !important;
            cursor: pointer !important;
        }

        div[data-testid="stButton"] > button:hover {
            text-decoration: underline !important;
            color: #0d5aa7 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.session_state.link_css_injected = True


# ============ CORE FUNCTIONS ============
def write_click_event(click: dict) -> None:
    """Log a click event (stub for Delta write)."""
    try:
        processed = {
            "session_id": click["session_id"],
            "message_id": click["message_id"],
            "link_id": click["link_id"],
            "url": click["url"],
            "document_title": extract_document_title(click["title_date"]),
            "document_date": extract_document_date(click["title_date"]),
            "broker_name": click["broker"],
            "clicked_at": click["click_time"],
            "written_at": datetime.utcnow().isoformat(),
            "user_id": os.getenv("USER", "unknown"),
        }
        print(f"✅ Would write to Delta: {processed}")
    except Exception as e:
        print(f"❌ Delta write failed: {e}")


def render_message_with_inline_icon_links(message: MockMessage, message_index: int):
    """Render inline clickable link icons inside message content."""
    pattern = r"\[(.*?)\s*—\s*Link\s*—\s*(.*?)\]\((.*?)\)"
    matches = list(re.finditer(pattern, message.content))

    if not matches:
        st.markdown(message.content)
        return

    last_pos = 0
    for idx, match in enumerate(matches):
        before = message.content[last_pos:match.start()]
        if before:
            st.markdown(before)

        title_date, broker, url = match.group(1).strip(), match.group(2).strip(), match.group(3).strip()
        link_id = hashlib.md5(f"{message.request_id}_{idx}_{url}".encode()).hexdigest()[:8]

        if not any(x["link_id"] == link_id for x in st.session_state.link_click_stats):
            st.session_state.link_click_stats.append(
                {
                    "link_id": link_id,
                    "url": url,
                    "message_id": message.request_id,
                    "title_date": title_date,
                    "broker": broker,
                    "session_id": st.session_state.session_id,
                    "clicked": False,
                    "click_time": None,
                }
            )
            
        st.markdown(
            f"""
            <div class='citation-line'>
                <span class='citation-inline'>
                    <span class='citation-frag'>{title_date}</span>
                    <span class='icon-link-wrapper' id='wrapper_{message_index}_{link_id}'></span>
                    <span class='citation-frag'>{broker}</span>
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Create inline button *in the same container*
        btn_key = f"icon_btn_{message_index}_{link_id}"
        placeholder = st.empty()

        # Inject button in same span using absolute alignment
        with placeholder.container():
            clicked = st.button("🔗", key=btn_key, help="Open source document")

        if clicked:
            for stat in st.session_state.link_click_stats:
                if stat["link_id"] == link_id:
                    stat["clicked"] = True
                    stat["click_time"] = datetime.utcnow().isoformat()
                    write_click_event(stat)
                    set_trace()
                    st.markdown(f"<script>window.open('{url}','_blank');</script>", unsafe_allow_html=True)
                    break

        last_pos = match.end()

    tail = message.content[last_pos:]
    if tail:
        st.markdown(tail)


def preprocess_link_stats() -> list[dict]:
    """Return processed stats for clicked links."""
    if not st.session_state.link_click_stats:
        return []

    processed_stats = []
    for stat in st.session_state.link_click_stats:
        if stat.get("click_time"):
            processed_stats.append(
                {
                    "session_id": stat["session_id"],
                    "message_id": stat["message_id"],
                    "link_id": stat["link_id"],
                    "url": stat["url"],
                    "document_title": extract_document_title(stat["title_date"]),
                    "document_date": extract_document_date(stat["title_date"]),
                    "broker_name": stat["broker"],
                    "clicked_at": stat["click_time"],
                    "written_at": datetime.utcnow().isoformat(),
                }
            )
    return processed_stats


# ============ UI ============
try:
    st.title("🧪 Link Click Tracking - Test Environment")
    st.markdown("---")

    # --- Controls ---
    st.markdown("### 🎛️ Test Controls")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔄 Reset Test", type="primary"):
            st.session_state.link_click_stats = []
            st.rerun()

    with col2:
        if st.button("📊 Show Stats"):
            total = len(st.session_state.link_click_stats)
            clicked = sum(1 for s in st.session_state.link_click_stats if s.get("clicked"))
            st.info(f"Total links parsed: {total}")
            st.success(f"Clicked links: {clicked}")

    st.markdown("---")

    # --- Chatbot Response Section ---
    st.markdown("### 🤖 Sample Chatbot Response")
    st.info("👇 Click the inline 🔗 icon to open the source and record the click without breaking text flow.")

    mock_message = MockMessage(content=SAMPLE_RESPONSE, request_id="test_msg_001")

    with st.container(border=True):
        render_message_with_inline_icon_links(mock_message, 0)

    st.markdown("---")

    # --- Tracking Stats ---
    st.markdown("### 📊 Link Tracking Statistics")

    if st.session_state.link_click_stats:
        # with st.expander("📋 Raw Link Metadata", expanded=True):
        #     st.json(st.session_state.link_click_stats)

        with st.expander("💾 Processed Stats (Ready for Delta Write)", expanded=True):
            processed = preprocess_link_stats()
            if processed:
                df = pd.DataFrame(processed)
                st.dataframe(df, use_container_width=True)
                st.success(f"✅ {len(processed)} clicked links ready to write")
            else:
                st.info("ℹ️ No clicked links yet. Click any Link above to generate stats.")

        with st.expander("🔍 Metadata Extraction Test"):
            for stat in st.session_state.link_click_stats[:2]:
                st.markdown(f"**Original:** `{stat['title_date']}`")
                st.markdown(f"- **Title:** {extract_document_title(stat['title_date'])}")
                st.markdown(f"- **Date:** {extract_document_date(stat['title_date'])}")
                st.markdown("---")
    else:
        st.info("ℹ️ No links tracked yet. The sample response will be rendered above with tracking enabled.")

    st.markdown("---")
    st.markdown(
        """
        ### 📝 Testing Notes

        - Each 'Link' is an inline Streamlit button styled as a hyperlink.
        - Clicking a link records: session_id, message_id, link_id, url, title, date, broker, timestamp.
        - The URL opens in a new browser tab immediately.
        - Use 'Show Stats' to verify counts; expand sections for raw and processed data.
        - 'Reset Test' clears all tracked metadata.

        ### ✅ Success Criteria
        - Link styling matches expected inline appearance.
        - Clicks reliably increment clicked count.
        - Processed stats show enriched fields (title/date).
        - No JavaScript dependency for core tracking.
        """
    )

    print("✅ UI rendered successfully!")

except Exception as e:
    st.error(f"Fatal error in main UI: {e}")
    print(f"❌ Fatal error: {e}")
    import traceback
    st.code(traceback.format_exc())
