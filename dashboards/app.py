import streamlit as st

# Subtle Palestinian-flag palette (muted)
PALETTE = {
    "green": "#0B6E4F",
    "red":   "#A23B3B",
    "black": "#1F1F1F",
    "white": "#F2F2F2",
    "gray":  "#6B7280",
}

st.set_page_config(page_title="Real-Time Social Network Analysis", layout="wide")

# Optional: light styling to make the app look consistent
st.markdown(
    f"""
    <style>
      .stApp {{
        background-color: {PALETTE["white"]};
        color: {PALETTE["black"]};
      }}
      .block-container {{
        padding-top: 2rem;
      }}
      /* Sidebar */
      section[data-testid="stSidebar"] {{
        background: #ffffff;
        border-right: 1px solid rgba(0,0,0,0.08);
      }}
      /* Headings */
      h1, h2, h3 {{
        color: {PALETTE["black"]};
      }}
      /* Subtle accents */
      .stMetric label {{
        color: {PALETTE["gray"]} !important;
      }}
    </style>
    """,
    unsafe_allow_html=True
)

st.title("Real-Time Social Network Analysis — Dashboards")
st.caption("Use the sidebar to navigate between dashboards.")

st.markdown(
    """
### What you have here
- **Posts & Comments (MongoDB)**: volume, engagement, and activity patterns.
- **Sentiment Analysis (JSON)**: sentiment distribution, model agreement, and text polarity insights.

### Quick checks (if something doesn’t load)
- MongoDB should be reachable from your Streamlit environment using your `MONGO_URI` in `secrets.toml`.
- The analyzed JSON file path should match `ANALYZED_COMMENTS_JSON` in `secrets.toml`.
"""
)

with st.expander("Show current data sources (from secrets.toml)", expanded=False):
    keys = ["MONGO_URI", "MONGO_DB", "POSTS_COLLECTION", "COMMENTS_COLLECTION", "ANALYZED_COMMENTS_JSON"]
    for k in keys:
        if k in st.secrets:
            # hide password-like URIs if you ever add creds later
            val = str(st.secrets[k])
            if "://" in val and "@" in val:
                st.write(f"- **{k}**: *(hidden)*")
            else:
                st.write(f"- **{k}**: `{val}`")
        else:
            st.write(f"- **{k}**: *(not set)*")

st.info("👉 Open a dashboard from the left sidebar under **Pages**.")
