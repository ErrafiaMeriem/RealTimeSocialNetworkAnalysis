import streamlit as st
import pandas as pd
import numpy as np
from pymongo import MongoClient
import plotly.express as px
from datetime import datetime, timezone

st.set_page_config(page_title="Posts & Comments (MongoDB)", layout="wide")

# ---------- Subtle Palestinian-flag palette (muted) ----------
PALETTE = {
    "green": "#0B6E4F",   # muted green
    "red":   "#A23B3B",   # muted red
    "black": "#1F1F1F",   # soft black
    "white": "#F2F2F2",   # off-white
    "gray":  "#6B7280",   # slate-ish
}
DISCRETE_SENT = {"positive": PALETTE["green"], "negative": PALETTE["red"], "neutral": PALETTE["gray"]}

def subtle_plotly_theme(fig):
    fig.update_layout(
        paper_bgcolor=PALETTE["white"],
        plot_bgcolor=PALETTE["white"],
        font=dict(color=PALETTE["black"]),
        title_font=dict(color=PALETTE["black"]),
        legend=dict(
            title=dict(font=dict(color=PALETTE["black"])),
            bgcolor=PALETTE["white"],
            font=dict(color=PALETTE["black"])
        ),
        margin=dict(l=10, r=10, t=45, b=10),
    )

    fig.update_xaxes(
        title_font=dict(color=PALETTE["black"]),
        tickfont=dict(color=PALETTE["black"]),
        gridcolor="rgba(0,0,0,0.08)",
        zerolinecolor="rgba(0,0,0,0.12)",
    )

    fig.update_yaxes(
        title_font=dict(color=PALETTE["black"]),
        tickfont=dict(color=PALETTE["black"]),
        gridcolor="rgba(0,0,0,0.08)",
        zerolinecolor="rgba(0,0,0,0.12)",
    )

    return fig


# ---------- Mongo helpers ----------
@st.cache_resource
def get_db():
    client = MongoClient(st.secrets["MONGO_URI"])
    return client[st.secrets["MONGO_DB"]]

db = get_db()
posts_col = db[st.secrets["POSTS_COLLECTION"]]
comments_col = db[st.secrets["COMMENTS_COLLECTION"]]

# ---------- Load data (light + safe) ----------
@st.cache_data(ttl=60)
def load_posts(limit=20000):
    cursor = posts_col.find(
        {}, {"_id": 0, "post_id": 1, "subreddit": 1, "post_text": 1, "score": 1, "created_at": 1, "post_hash": 1}
    ).limit(limit)
    df = pd.DataFrame(list(cursor))
    if not df.empty and "created_at" in df:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["score"] = pd.to_numeric(df.get("score", 0), errors="coerce").fillna(0)
    df["subreddit"] = df.get("subreddit", "").fillna("Unknown")
    return df

@st.cache_data(ttl=60)
def load_comments(limit=50000):
    cursor = comments_col.find(
        {}, {"_id": 0, "comment_id": 1, "post_id": 1, "clean_comment": 1, "score": 1, "created_at": 1, "comment_hash": 1, "parent_id": 1}
    ).limit(limit)
    df = pd.DataFrame(list(cursor))
    if not df.empty and "created_at" in df:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["score"] = pd.to_numeric(df.get("score", 0), errors="coerce").fillna(0)
    return df

# Sidebar controls
st.sidebar.header("Filters")
time_grain = st.sidebar.selectbox("Time granularity", ["Hour", "Day"], index=1)
top_n = st.sidebar.slider("Top N subreddits", 5, 30, 10)

df_posts = load_posts()
df_comments = load_comments()

# If you have multiple subreddits, allow filter
all_subs = sorted(df_posts["subreddit"].dropna().unique().tolist()) if not df_posts.empty else []
selected_subs = st.sidebar.multiselect("Subreddits", options=all_subs, default=all_subs)

if selected_subs:
    df_posts_f = df_posts[df_posts["subreddit"].isin(selected_subs)].copy()
else:
    df_posts_f = df_posts.copy()

# Comments may or may not have subreddit; join via post_id if needed
if not df_comments.empty:
    if "subreddit" not in df_comments.columns:
        df_comments_f = df_comments.merge(df_posts_f[["post_id", "subreddit"]], on="post_id", how="left")
        df_comments_f["subreddit"] = df_comments_f["subreddit"].fillna("Unknown")
    else:
        df_comments_f = df_comments.copy()
        df_comments_f = df_comments_f[df_comments_f["subreddit"].isin(selected_subs)] if selected_subs else df_comments_f
else:
    df_comments_f = df_comments.copy()

# ---------- Title ----------
st.title("Posts & Comments Dashboard")
st.caption("Source: MongoDB collections `posts_clean` and `comments_clean`")

# ---------- KPIs ----------
posts_count = int(posts_col.count_documents({}))
comments_count = int(comments_col.count_documents({}))

subreddit_count = int(df_posts["subreddit"].nunique()) if not df_posts.empty else 0
posts_in_filter = len(df_posts_f)
comments_in_filter = len(df_comments_f)

# Replies detection (only if parent_id exists)
has_parent = "parent_id" in df_comments_f.columns and df_comments_f["parent_id"].notna().any()
reply_count = None
comment_root_count = None
if has_parent:
    # common reddit conventions: t3_... = reply to post (i.e., top-level comment), t1_... = reply to a comment
    parent_str = df_comments_f["parent_id"].astype(str)
    comment_root_count = int(parent_str.str.startswith("t3").sum())
    reply_count = int(parent_str.str.startswith("t1").sum())

# Dedup rate based on hashes (if available)
def dedup_stats(df, hash_col):
    if df.empty or hash_col not in df.columns:
        return None
    total = len(df)
    unique = df[hash_col].nunique(dropna=True)
    if unique == 0:
        return None
    dups = total - unique
    return total, unique, dups, (dups / total) * 100

post_dedup = dedup_stats(df_posts_f, "post_hash")
comment_dedup = dedup_stats(df_comments_f, "comment_hash")

k1, k2, k3, k4 = st.columns(4)

k1.metric("Subreddits", f"{subreddit_count:,}")
k2.metric("Posts", f"{posts_count:,}", help="Total documents in MongoDB")
k3.metric("Comments", f"{comments_count:,}", help="Total documents in MongoDB")
k4.metric("Posts (filtered)", f"{posts_in_filter:,}")

# ---------- Engagement join: comments per post ----------
st.divider()

st.subheader("Engagement overview")

comments_per_post = (
    df_comments_f.groupby("post_id", as_index=False)
    .agg(comment_count=("comment_id", "count"), avg_comment_score=("score", "mean"))
)

posts_eng = df_posts_f.merge(comments_per_post, on="post_id", how="left")
posts_eng["comment_count"] = posts_eng["comment_count"].fillna(0).astype(int)
posts_eng["avg_comment_score"] = posts_eng["avg_comment_score"].fillna(0)

cA, cB = st.columns(2)

with cA:
    # Top subreddits by posts
    if not df_posts_f.empty:
        top_posts_sub = (
            df_posts_f.groupby("subreddit", as_index=False)
            .size()
            .rename(columns={"size": "posts"})
            .sort_values("posts", ascending=False)
            .head(top_n)
        )
        fig = px.bar(
            top_posts_sub,
            x="subreddit",
            y="posts",
            title=f"Top {top_n} subreddits by number of posts",
            color_discrete_sequence=[PALETTE["green"]],
        )
        fig = subtle_plotly_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No posts available for the selected filters.")

with cB:
    # Top subreddits by comments
    if not df_comments_f.empty:
        top_comments_sub = (
            df_comments_f.groupby("subreddit", as_index=False)
            .size()
            .rename(columns={"size": "comments"})
            .sort_values("comments", ascending=False)
            .head(top_n)
        )
        fig = px.bar(
            top_comments_sub,
            x="subreddit",
            y="comments",
            title=f"Top {top_n} subreddits by number of comments",
            color_discrete_sequence=[PALETTE["red"]],
        )
        fig = subtle_plotly_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No comments available for the selected filters.")

# ---------- Activity over time ----------
st.divider()
st.subheader("Activity over time")

def make_time_series(df, dt_col, label):
    if df.empty or dt_col not in df.columns or df[dt_col].isna().all():
        return pd.DataFrame(columns=["time", label])
    freq = "H" if time_grain == "Hour" else "D"
    ts = df.dropna(subset=[dt_col]).set_index(dt_col).resample(freq).size().reset_index(name=label)
    ts.rename(columns={dt_col: "time"}, inplace=True)
    return ts

ts_posts = make_time_series(df_posts_f, "created_at", "posts")
ts_comments = make_time_series(df_comments_f, "created_at", "comments")

ts = ts_posts.merge(ts_comments, on="time", how="outer").fillna(0).sort_values("time")
ts["posts"] = ts["posts"].astype(int)
ts["comments"] = ts["comments"].astype(int)

col1, col2 = st.columns(2)
with col1:
    fig = px.line(ts, x="time", y="posts", title=f"Posts per {time_grain.lower()}", markers=False,
                  color_discrete_sequence=[PALETTE["green"]])
    fig = subtle_plotly_theme(fig)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.line(ts, x="time", y="comments", title=f"Comments per {time_grain.lower()}", markers=False,
                  color_discrete_sequence=[PALETTE["red"]])
    fig = subtle_plotly_theme(fig)
    st.plotly_chart(fig, use_container_width=True)

# ---------- Engagement scatter ----------
st.divider()
st.subheader("What drives engagement?")

# Helpful “insight” features
posts_eng["post_length"] = posts_eng["post_text"].fillna("").astype(str).str.len()
posts_eng["score"] = pd.to_numeric(posts_eng["score"], errors="coerce").fillna(0)

# Scatter: post score vs comment_count (size = post_length)
fig = px.scatter(
    posts_eng,
    x="score",
    y="comment_count",
    hover_data=["subreddit", "post_id"],
    size=np.clip(posts_eng["post_length"], 1, None),
    title="Post score vs number of comments (bubble size ≈ post length)",
    color="subreddit" if len(selected_subs) <= 8 else None,  # avoid too many colors
    color_discrete_sequence=[PALETTE["green"], PALETTE["red"], PALETTE["black"], PALETTE["gray"]],
)
fig = subtle_plotly_theme(fig)
st.plotly_chart(fig, use_container_width=True)

# ---------- Activity heatmap (day of week x hour) ----------
st.divider()
st.subheader("When are people most active?")

def heatmap_counts(df, dt_col, value_name):
    if df.empty or dt_col not in df.columns or df[dt_col].isna().all():
        return None
    tmp = df.dropna(subset=[dt_col]).copy()
    tmp["dow"] = tmp[dt_col].dt.day_name()
    tmp["hour"] = tmp[dt_col].dt.hour
    # order days
    dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    pivot = tmp.pivot_table(index="dow", columns="hour", values=value_name, aggfunc="count", fill_value=0)
    pivot = pivot.reindex(dow_order)
    return pivot

hm_comments = heatmap_counts(df_comments_f, "created_at", "comment_id")
if hm_comments is not None:
    # Use a subtle single-hue colorscale (muted green)
    fig = px.imshow(
        hm_comments,
        aspect="auto",
        title="Comments activity heatmap (Day of week × Hour)",
        color_continuous_scale=[[0, PALETTE["white"]], [1, PALETTE["green"]]],
    )
    fig = subtle_plotly_theme(fig)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Heatmap needs valid `created_at` timestamps in comments.")

# ---------- Quality / dedup panel ----------
st.divider()
st.subheader("Data quality quick check (hash-based duplicates)")

q1, q2 = st.columns(2)
with q1:
    if post_dedup:
        total, unique, dups, pct = post_dedup
        st.write("**Posts (filtered)**")
        st.write(f"- Total: **{total:,}**")
        st.write(f"- Unique by `post_hash`: **{unique:,}**")
        st.write(f"- Potential duplicates: **{dups:,}** ({pct:.2f}%)")
    else:
        st.write("**Posts**: no `post_hash` found or not enough data to compute duplicates.")

with q2:
    if comment_dedup:
        total, unique, dups, pct = comment_dedup
        st.write("**Comments (filtered)**")
        st.write(f"- Total: **{total:,}**")
        st.write(f"- Unique by `comment_hash`: **{unique:,}**")
        st.write(f"- Potential duplicates: **{dups:,}** ({pct:.2f}%)")
    else:
        st.write("**Comments**: no `comment_hash` found or not enough data to compute duplicates.")

# ---------- Top content tables ----------
st.divider()
st.subheader("Top content")

t1, t2 = st.columns(2)

with t1:
    st.write("**Top posts by score**")
    top_posts = posts_eng.sort_values(["score", "comment_count"], ascending=False).head(10).copy()
    top_posts["post_preview"] = top_posts["post_text"].fillna("").astype(str).str.slice(0, 140) + "…"
    st.dataframe(
        top_posts[["subreddit", "post_id", "score", "comment_count", "post_preview", "created_at"]],
        use_container_width=True,
        hide_index=True,
    )

with t2:
    st.write("**Most upvoted comments**")
    if not df_comments_f.empty:
        top_comments = df_comments_f.sort_values("score", ascending=False).head(10).copy()
        top_comments["comment_preview"] = top_comments["clean_comment"].fillna("").astype(str).str.slice(0, 160) + "…"
        cols = ["subreddit", "post_id", "comment_id", "score", "comment_preview", "created_at"]
        cols = [c for c in cols if c in top_comments.columns]
        st.dataframe(top_comments[cols], use_container_width=True, hide_index=True)
    else:
        st.info("No comments to display.")

st.caption("Tip: If you want true replies vs comments counts, store `parent_id` in your cleaned collection (t3 = top-level comment, t1 = reply).")
