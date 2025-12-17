"""
Standalone Streamlit test app for link click tracking functionality.

Run with:
    streamlit run app_click_test.py
"""
from streamlit_js_eval import streamlit_js_eval
import streamlit.components.v1 as components
import json
import os
import re
import sys
import hashlib
from datetime import datetime
import urllib.parse

import pandas as pd
import streamlit as st
from pdb import set_trace


# ============ INITIALIZATION ============
print("✅ Script started successfully!")
sys.stdout.flush()

SAMPLE_RESPONSE = """
Industry CoWoS capacity is expected to grow from 756,000 wafers in 2025 to 1,170,000 wafers in 2026, with TSMC maintaining the majority share alongside non-TSMC providers like Amkor, UMC, and ASE [NVIDIA Corp "A Bit Noisy, But Strong Demand Should Carry The Day; Buy" (Buy) (2025-08-28) — Link — UBS](https://example.com/ubs-report-1)

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
        .citation-line { margin: 0 0 .1rem 0 !important; padding: 0 !important; line-height: 1.15 !important; }
        .citation-frag { display: inline !important; vertical-align: middle !important; }
        .citation-line .icon-link-wrapper { margin: 0 0.35rem 0 0.35rem; display: inline !important; }
        .inline-link {
            text-decoration: none;
            background: transparent;
            border-radius: 4px;
            padding: 0 4px;
            font-size: inherit;
            line-height: inherit;
            color: #1f77b4;
            border: 1px solid rgba(255,255,255,0.04);
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 1.2rem;
            min-height: 1.2rem;
        }
        .inline-link:hover { text-decoration: underline; color: #0d5aa7; }
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


# def render_message_with_inline_icon_links(message: MockMessage, message_index: int):
#     """Render inline clickable link icons inside message content."""
#     pattern = r"\[(.*?)\s*—\s*Link\s*—\s*(.*?)\]\((.*?)\)"
#     matches = list(re.finditer(pattern, message.content))

#     if not matches:
#         st.markdown(message.content)
#         return
    
#     rendered_html = ""
#     last_pos = 0
    
#     for idx, match in enumerate(matches):
#         before = message.content[last_pos:match.start()]
#         # if before:
#         #     st.markdown(before)
#         rendered_html += f"<span>{before}</span>"

#         title_date, broker, url = match.group(1).strip(), match.group(2).strip(), match.group(3).strip()
        
#         # 3️⃣ Generate unique link ID and store metadata
#         link_id = hashlib.md5(f"{message.request_id}_{idx}_{url}".encode()).hexdigest()[:8]

#         if not any(x["link_id"] == link_id for x in st.session_state.link_click_stats):
#             st.session_state.link_click_stats.append(
#                 {
#                     "link_id": link_id,
#                     "url": url,
#                     "message_id": message.request_id,
#                     "title_date": title_date,
#                     "broker": broker,
#                     "session_id": st.session_state.session_id,
#                     "clicked": False,
#                     "click_time": None,
#                 }
#             )
            
#         # Unique click key (useful to log)
#         click_key = f"msg{message_index}_link{link_id}"

#         # Build inline HTML with anchor that appends a query param when clicked.
#         # The timestamp parameter `_` prevents aggressive caching in some browsers.
#         timestamp = int(datetime.utcnow().timestamp())
#         anchor_href = f"?click={urllib.parse.quote(click_key)}&_={timestamp}"

#         # Inline HTML (same span line)
#         rendered_html += (
#             f"<span class='citation-line'>"
#             f"<span class='citation-frag'>{title_date}</span>"
#             f"<span class='citation-frag icon-link-wrapper'>"
#             f"<a class='inline-link' href='{anchor_href}' title='Open source document'>🔗</a>"
#             f"</span>"
#             f"<span class='citation-frag'>{broker}</span>"
#             f"</span>"
#         )

#     #     last_pos = match.end()
        
#     #     # 5️⃣ Append the tail content after all matches
#     # tail = message.content[last_pos:]
#     # if tail:
#     #     rendered_html += f"<span>{tail}</span>"

#     # # 6️⃣ Render final composed HTML as one markdown block
#     # st.markdown(rendered_html, unsafe_allow_html=True)

#     # # 7️⃣ Handle click detection
#     # params = st.query_params
#     # clicked = False
#     # clicked_key = None
        
#         # # --- Handle the "click" action ---
#     params = st.query_params
#         # clicked = False

#     if "click" in params:
#         clicked_key = params.get("click", [""])#[0]
#         clicked = bool(clicked_key)
#         # --- Click processing logic (adapted from your code) ---
#     if clicked:
#         # set_trace()
#         for stat in st.session_state.link_click_stats:
#             key_match = f"msg{message_index}_link{stat['link_id']}"
#             if key_match == clicked_key:
#                 stat["clicked"] = True
#                 stat["click_time"] = datetime.utcnow().isoformat()
#                 print(stat)
#                 write_click_event(stat)
#                 preprocess_link_stats()

#                 # Open link in a new tab
#                 set_trace()
#                 st.markdown(
#                     f"<script>window.open('{stat['url']}', '_blank');</script>",
#                     unsafe_allow_html=True,
#                 )
#                 break

        # Clear query params to prevent re-triggering
        # st.query_params.clear()
        # st.rerun()
            

        #     # Clear query parameters so URL resets cleanly after action
        #     st.experimental_set_query_params()
        #     st.rerun()

        # # # --- Optional tail rendering ---
        # last_pos = match.end()
        # tail = message.content[last_pos:] 
        # if tail: 
        #     st.markdown(tail)
        
        
# def render_message_with_inline_icon_links(message, message_index: int):
#     """Render inline clickable link icons inside message content and handle click events."""
#     pattern = r"\[(.*?)\s*—\s*Link\s*—\s*(.*?)\]\((.*?)\)"
#     matches = list(re.finditer(pattern, message.content))

#     # If no inline link patterns, just render message
#     if not matches:
#         st.markdown(message.content)
#         return

#     rendered_html = ""
#     last_pos = 0

#     # --- Render message content and embed clickable links ---
#     for idx, match in enumerate(matches):
#         before = message.content[last_pos:match.start()]
#         rendered_html += f"<span>{before}</span>"

#         title_date, broker, url = (
#             match.group(1).strip(),
#             match.group(2).strip(),
#             match.group(3).strip(),
#         )

#         # Generate unique ID for each link
#         link_id = hashlib.md5(f"{message.request_id}_{idx}_{url}".encode()).hexdigest()[:8]

#         # Store click metadata once per session
#         if not any(x["link_id"] == link_id for x in st.session_state.link_click_stats):
#             st.session_state.link_click_stats.append(
#                 {
#                     "link_id": link_id,
#                     "url": url,
#                     "message_id": message.request_id,
#                     "title_date": title_date,
#                     "broker": broker,
#                     "session_id": st.session_state.session_id,
#                     "clicked": False,
#                     "click_time": None,
#                 }
#             )

#         click_key = f"msg{message_index}_link{link_id}"
#         timestamp = int(datetime.utcnow().timestamp())
        
#         anchor_href = f"?click={stat['url']}"

#         rendered_html += (
#             f"<span class='citation-line'>"
#             f"<span class='citation-frag'>{title_date}</span>"
#             f"<span class='citation-frag icon-link-wrapper'>"
#             f"<a class='inline-link' href='{anchor_href}' title='Open source document'>🔗</a>"
#             f"</span>"
#             f"<span class='citation-frag'>{broker}</span>"
#             f"</span>"
#         )
#         last_pos = match.end()
        
#     # # Append remaining tail text
#     # Append remaining text after last match
#     tail = message.content[last_pos:]
#     if tail:
#         rendered_html += f"{tail}"

#     # Render entire composed HTML
#     st.markdown(rendered_html, unsafe_allow_html=True)

#     # --- Handle click detection (new API only) ---
#     params = st.query_params
#     clicked_key = params.get("click")
#     clicked = bool(clicked_key)

#     if clicked:
#         set_trace()
#         clicked_key = clicked_key[0] if isinstance(clicked_key, list) else clicked_key

#         for stat in st.session_state.link_click_stats:
#             key_match = f"msg{message_index}_link{stat['link_id']}"
#             if key_match == clicked_key:
#                 stat["clicked"] = True
#                 stat["click_time"] = datetime.utcnow().isoformat()
#                 write_click_event(stat)
#                 # preprocess_link_stats()

#                 # Open link in new tab via JS
#                 st.markdown(
#                     f"<script>window.open('{stat['url']}', '_blank');</script>",
#                     unsafe_allow_html=True,
#                 )
#                 break
#         st.query_params.clear()
#         st.rerun()

def render_message_with_inline_icon_links(message, message_index: int):
    """Render inline clickable link icons using query params for click detection."""
    pattern = r"\[(.*?)\s*—\s*Link\s*—\s*(.*?)\]\((.*?)\)"
    matches = list(re.finditer(pattern, message.content))

    if not matches:
        st.markdown(message.content)
        return

    rendered_html = ""
    last_pos = 0

    for idx, match in enumerate(matches):
        before = message.content[last_pos:match.start()]
        rendered_html += f"<span>{before}</span>"

        title_date, broker, url = (
            match.group(1).strip(),
            match.group(2).strip(),
            match.group(3).strip(),
        )

        # Unique ID per link
        link_id = hashlib.md5(f"{message.request_id}_{idx}_{url}".encode()).hexdigest()[:8]
        click_key = f"msg{message_index}_link{link_id}"

        # Save link metadata once
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

        # Inline clickable anchor with query param
        timestamp = int(datetime.utcnow().timestamp())
        set_trace()
        anchor_href = f"?click={urllib.parse.quote(click_key)}&_={timestamp}"
        
        rendered_html += (
            f"<span class='citation-line' style='white-space: nowrap;'>"
            f"<span class='citation-frag'>{title_date}</span> "
            # f"<a class='inline-link' href='{anchor_href}' title='Open source document'>🔗</a> "
            f"<a href='{url}' target='_blank' onclick=\"fetch('/_log_click', {{method:'POST', body:JSON.stringify({{'id':'msg0_link123'}})}})\">🔗</a>"
            f"<span class='citation-frag'>{broker}</span>"
            f"</span>"
        )
        last_pos = match.end()

    # Append tail text
    
    tail = message.content[last_pos:]
    if tail:
        rendered_html += f"<span>{tail}</span>"

    # Render entire message
    st.markdown(rendered_html, unsafe_allow_html=True)

    # --- Handle click ---
    params = st.query_params
    clicked_key = params.get("click")
    clicked = bool(clicked_key)

    if clicked:
        set_trace()
        
        clicked_key = clicked_key[0] if isinstance(clicked_key, list) else clicked_key

        for stat in st.session_state.link_click_stats:
            key_match = f"msg{message_index}_link{stat['link_id']}"
            if key_match == clicked_key:
                stat["clicked"] = True
                stat["click_time"] = datetime.utcnow().isoformat()
                write_click_event(stat)
                set_trace()
                st.markdown(f"<script>window.open('{stat['url']}', '_blank');</script>",unsafe_allow_html=True,)
                break

        # # clear and rerun to reset URL
        # st.query_params.clear()
        # st.rerun()
        
        
# def render_message_with_inline_icon_links(message, message_index: int):
#     """Render inline clickable link icons that open the real URL and log clicks without rerun."""
#     pattern = r"\[(.*?)\s*—\s*Link\s*—\s*(.*?)\]\((.*?)\)"
#     matches = list(re.finditer(pattern, message.content))

#     if not matches:
#         st.markdown(message.content)
#         return

#     html_content = ""
#     last_pos = 0

#     for idx, match in enumerate(matches):
#         before = message.content[last_pos:match.start()]
#         html_content += before

#         title_date, broker, url = (
#             match.group(1).strip(),
#             match.group(2).strip(),
#             match.group(3).strip(),
#         )

#         link_id = hashlib.md5(f"{message.request_id}_{idx}_{url}".encode()).hexdigest()[:8]
#         click_key = f"msg{message_index}_link{link_id}"

#         # Track metadata if new
#         if not any(x["link_id"] == link_id for x in st.session_state.link_click_stats):
#             st.session_state.link_click_stats.append(
#                 {
#                     "link_id": link_id,
#                     "url": url,
#                     "message_id": message.request_id,
#                     "title_date": title_date,
#                     "broker": broker,
#                     "session_id": st.session_state.session_id,
#                     "clicked": False,
#                     "click_time": None,
#                 }
#             )

#         # Inline HTML — the <a> directly opens the real URL
#         html_content += (
#             f"<span class='citation-line' style='white-space: nowrap;'>"
#             f"<span class='citation-frag'>{title_date}</span> "
#             f"<a id='{click_key}' href='{url}' target='_blank' "
#             f"class='inline-link' title='Open source document'>🔗</a> "
#             f"<span class='citation-frag'>{broker}</span>"
#             f"</span>"
#         )
#         last_pos = match.end()

#     tail = message.content[last_pos:]
#     if tail:
#         html_content += tail

#     # Add JS to intercept link clicks for logging only
#     js = """
#     <script>
#     document.querySelectorAll('.inline-link').forEach(link => {
#         link.addEventListener('click', (e) => {
#             const id = e.target.id;
#             const href = e.target.href;
#             fetch('/_stcorelog', {
#                 method: 'POST',
#                 body: JSON.stringify({event: "link_click", id: id, href: href}),
#                 headers: {'Content-Type': 'application/json'}
#             }).catch(err => console.error('Logging failed', err));
#         });
#     });
#     </script>
#     """

#     html = f"""
#     <html>
#     <head>
#       <style>
#         body {{ margin:0; font-family:inherit; color:inherit; background:transparent; }}
#         .citation-line {{ display:inline-flex; align-items:center; gap:6px; }}
#         .inline-link {{ text-decoration:none; cursor:pointer; }}
#       </style>
#     </head>
#     <body>
#       {html_content}
#       {js}
#     </body>
#     </html>
#     """

#     # Render HTML inside iframe so script executes
#     components.html(html, height=200, scrolling=False)

#     # Log click data if the app receives a request (optional fallback)
#     clicked_event = st.session_state.get("clicked_event")
#     if clicked_event:
#         try:
#             evt = json.loads(clicked_event)
#         except Exception:
#             evt = None
#         if evt and evt.get("event") == "link_click":
#             clicked_key = evt["id"]
#             for stat in st.session_state.link_click_stats:
#                 key_match = f"msg{message_index}_link{stat['link_id']}"
#                 if key_match == clicked_key:
#                     stat["clicked"] = True
#                     stat["click_time"] = datetime.utcnow().isoformat()
#                     write_click_event(stat)
#                     break
#             st.session_state.clicked_event = None



def render_message_with_inline_icon_links(message, message_index: int):
    pattern = r"\[(.*?)\s*—\s*Link\s*—\s*(.*?)\]\((.*?)\)"
    matches = list(re.finditer(pattern, message.content))
    if not matches:
        st.markdown(message.content)
        return

    rendered_html = ""
    last_pos = 0

    for idx, match in enumerate(matches):
        before = message.content[last_pos:match.start()]
        rendered_html += before

        title_date, broker, url = (
            match.group(1).strip(),
            match.group(2).strip(),
            match.group(3).strip(),
        )
        link_id = hashlib.md5(f"{message.request_id}_{idx}_{url}".encode()).hexdigest()[:8]
        click_key = f"msg{message_index}_link{link_id}"

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

        rendered_html += (
            f"<span style='white-space: nowrap;'>"
            f"<span>{title_date}</span> "
            f"<a href='{url}' target='_blank' "
            f"onclick=\"window.clickedLinkId='{click_key}';\">🔗</a> "
            f"<span>{broker}</span>"
            f"</span>"
        )

        last_pos = match.end()

    tail = message.content[last_pos:]
    if tail:
        rendered_html += tail

    st.markdown(rendered_html, unsafe_allow_html=True)

    # Capture JS variable back to Python
    clicked_link = streamlit_js_eval(js_expressions="window.clickedLinkId", key=f"click_eval_{message_index}")

    if clicked_link:
        for stat in st.session_state.link_click_stats:
            if f"msg{message_index}_link{stat['link_id']}" == clicked_link:
                stat["clicked"] = True
                stat["click_time"] = datetime.utcnow().isoformat()
                write_click_event(stat)
                break
            
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
