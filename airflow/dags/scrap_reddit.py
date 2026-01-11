import requests
import time
import os
import json
import logging
from datetime import datetime


# LOGGING

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# CONFIGURATION

SUBREDDITS = [
    "Palestine",
    "IsraelPalestine",
    "IsraelUnderAttack"
]

QUERY = "palestine OR israel OR gaza"
LIMIT_POSTS = 20
SLEEP_TIME = 2
MAX_REPLIES = 4

REVISIT_WINDOW_SECONDS = 2 * 60 * 60

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

CHECKPOINT_FILE = os.path.join(DATA_DIR, "last_checkpoint.json")
POSTS_FILE = os.path.join(DATA_DIR, "posts.json")
COMMENTS_FILE = os.path.join(DATA_DIR, "comments.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json"
}


# CHECKPOINT
def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return {}

    try:
        with open(CHECKPOINT_FILE, "r") as f:
            content = f.read().strip()
            return json.loads(content) if content else {}
    except json.JSONDecodeError:
        logger.warning("⚠️ Checkpoint corrompu, réinitialisation.")
        return {}

def save_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2)


# JSON APPEND
def append_to_json(path, new_data):
    if not new_data:
        return

    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []
    else:
        existing = []

    existing.extend(new_data)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)


# POSTS
def scrape_posts(subreddit, last_ts):
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    params = {
        "q": QUERY,
        "restrict_sr": 1,
        "sort": "new",
        "limit": LIMIT_POSTS
    }

    logger.info(f"➡️ Scraping posts from r/{subreddit}")

    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
    except Exception as e:
        logger.error(e)
        return [], last_ts

    if r.status_code != 200:
        return [], last_ts

    children = r.json().get("data", {}).get("children", [])
    posts = []
    max_ts = last_ts

    for item in children:
        p = item["data"]
        created_ts = p["created_utc"]

        if last_ts and created_ts < last_ts - REVISIT_WINDOW_SECONDS:
            continue

        max_ts = max(max_ts, created_ts)

        posts.append({
            "post_id": p["id"],
            "subreddit": subreddit,
            "title": p["title"],
            "selftext": p.get("selftext", ""),
            "score": p["score"],
            "num_comments": p["num_comments"],
            "created_at": datetime.utcfromtimestamp(created_ts).isoformat()
        })

    logger.info(f"New posts: {len(posts)}")
    return posts, max_ts


# COMMENTS & REPLIES
def parse_comments(children, subreddit, post_id, last_ts, depth=0):
    if depth >= MAX_REPLIES:
        return [], last_ts

    results = []
    max_ts = last_ts

    for c in children:
        if c["kind"] != "t1":
            continue

        d = c["data"]
        created_ts = d["created_utc"]

        if last_ts and created_ts <= last_ts:
            continue

        max_ts = max(max_ts, created_ts)

        results.append({
            "comment_id": d["id"],
            "post_id": post_id,
            "subreddit": subreddit,
            "body": d["body"],
            "score": d["score"],
            "parent_id": d["parent_id"],
            "created_at": datetime.utcfromtimestamp(created_ts).isoformat()
        })

        replies = d.get("replies")
        if isinstance(replies, dict):
            sub, sub_ts = parse_comments(
                replies["data"]["children"],
                subreddit,
                post_id,
                last_ts,
                depth + 1
            )
            results.extend(sub)
            max_ts = max(max_ts, sub_ts)

    return results, max_ts

def scrape_comments(post_id, subreddit, last_ts):
    url = f"https://www.reddit.com/comments/{post_id}.json"

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
    except Exception:
        return [], last_ts

    if r.status_code != 200:
        return [], last_ts

    top_level = r.json()[1]["data"]["children"]
    return parse_comments(top_level, subreddit, post_id, last_ts)


# PIPELINE
def scrape():
    checkpoint = load_checkpoint()
    all_posts = []
    all_comments = []

    logger.info("🚀 Reddit scraping started")

    for subreddit in SUBREDDITS:
        last_ts = checkpoint.get(subreddit, 0)
        logger.info(f"r/{subreddit} | last checkpoint = {last_ts}")

        posts, post_max_ts = scrape_posts(subreddit, last_ts)
        all_posts.extend(posts)

        comment_max_ts = last_ts

        for post in posts:
            time.sleep(SLEEP_TIME)
            comments, ts = scrape_comments(post["post_id"], subreddit, last_ts)
            all_comments.extend(comments)
            comment_max_ts = max(comment_max_ts, ts)

        checkpoint[subreddit] = max(post_max_ts, comment_max_ts)
        time.sleep(SLEEP_TIME)

    save_checkpoint(checkpoint)

    append_to_json(POSTS_FILE, all_posts)
    append_to_json(COMMENTS_FILE, all_comments)

    logger.info(f"TOTAL posts: {len(all_posts)}")
    logger.info(f"TOTAL comments + replies: {len(all_comments)}")

    return all_posts, all_comments


# MANUAL RUN

if __name__ == "__main__":
    scrape()
