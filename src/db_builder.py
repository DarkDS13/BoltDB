#!/usr/bin/env python3
"""Build and seed a SQL-heavy social media DBMS dataset for project demos."""

from __future__ import annotations

import argparse
import random
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql"
DEFAULT_DB_PATH = ROOT / "data" / "social_media.db"

DEVICE_TYPES = ("web", "ios", "android")
MEDIA_TYPES = ("text", "image", "video", "link")

CITIES = (
    "Ahmedabad",
    "Surat",
    "Mumbai",
    "Pune",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Kolkata",
    "Jaipur",
)

TOPICS = (
    "technology",
    "sports",
    "music",
    "gaming",
    "education",
    "finance",
    "travel",
    "food",
    "movies",
    "health",
)

TOPIC_TAGS = {
    "technology": ["ai", "cloud", "cybersecurity", "startup", "coding"],
    "sports": ["cricket", "football", "fitness", "ipl", "highlights"],
    "music": ["indie", "live", "album", "concert", "playlist"],
    "gaming": ["esports", "fps", "rpg", "speedrun", "stream"],
    "education": ["dbms", "sql", "exam", "project", "learning"],
    "finance": ["stocks", "crypto", "investing", "economy", "fintech"],
    "travel": ["roadtrip", "adventure", "mountains", "beach", "itinerary"],
    "food": ["recipe", "streetfood", "restaurant", "dessert", "coffee"],
    "movies": ["trailer", "cinema", "review", "ott", "filmmaking"],
    "health": ["nutrition", "workout", "mentalhealth", "wellness", "sleep"],
}

POST_PHRASES = (
    "Quick update on",
    "Thoughts about",
    "Breaking down",
    "My take on",
    "Latest trend in",
    "Deep dive into",
    "New perspective on",
)

COMMENT_PHRASES = (
    "Great point about",
    "Interesting angle on",
    "I disagree a bit on",
    "Can you explain more about",
    "This helped me understand",
    "Strongly agree with your note on",
)


@dataclass
class BuildConfig:
    users: int
    posts: int
    likes: int
    comments: int
    follows: int
    logins_per_user: int
    seed: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create and seed the social media DBMS demo database.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to SQLite database file.")
    parser.add_argument("--users", type=int, default=3000, help="Number of users to generate.")
    parser.add_argument("--posts", type=int, default=18000, help="Number of posts to generate.")
    parser.add_argument("--likes", type=int, default=90000, help="Number of likes to generate.")
    parser.add_argument("--comments", type=int, default=45000, help="Number of comments to generate.")
    parser.add_argument("--follows", type=int, default=22000, help="Number of follow edges to generate.")
    parser.add_argument(
        "--logins-per-user",
        type=int,
        default=12,
        help="Approximate number of login activity records to create per user.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument("--rebuild", action="store_true", help="Delete existing database before building.")
    return parser.parse_args()


def load_sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


def timestamp_from_epoch(epoch_value: int) -> str:
    return datetime.fromtimestamp(epoch_value, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def rand_epoch(rng: random.Random, start_epoch: int, end_epoch: int) -> int:
    return rng.randint(start_epoch, end_epoch)


def chunked_rows(rows: Sequence[tuple], chunk_size: int = 5000) -> Iterable[Sequence[tuple]]:
    for i in range(0, len(rows), chunk_size):
        yield rows[i : i + chunk_size]


def timed(message: str) -> float:
    print(f"[+] {message}")
    return time.perf_counter()


def stop_timer(start_time: float, message: str) -> None:
    elapsed = time.perf_counter() - start_time
    print(f"    -> {message} in {elapsed:.2f}s")


def generate_users(cfg: BuildConfig, rng: random.Random, now_epoch: int) -> list[tuple]:
    joined_start = now_epoch - 360 * 24 * 3600
    joined_end = now_epoch - 10 * 24 * 3600

    users = []
    for user_id in range(1, cfg.users + 1):
        joined_at = timestamp_from_epoch(rand_epoch(rng, joined_start, joined_end))
        users.append(
            (
                user_id,
                f"user_{user_id:05d}",
                f"user_{user_id:05d}@example.com",
                rng.choice(CITIES),
                f"I enjoy talking about {rng.choice(TOPICS)}.",
                joined_at,
                1 if rng.random() < 0.08 else 0,
            )
        )
    return users


def generate_follows(cfg: BuildConfig, rng: random.Random, now_epoch: int) -> list[tuple]:
    start_epoch = now_epoch - 240 * 24 * 3600
    follows: list[tuple] = []
    seen: set[tuple[int, int]] = set()

    while len(follows) < cfg.follows:
        follower = rng.randint(1, cfg.users)
        followee = rng.randint(1, cfg.users)
        if follower == followee:
            continue
        key = (follower, followee)
        if key in seen:
            continue
        seen.add(key)
        follows.append(
            (
                follower,
                followee,
                rng.choice(DEVICE_TYPES),
                timestamp_from_epoch(rand_epoch(rng, start_epoch, now_epoch)),
            )
        )
    return follows


def generate_posts(
    cfg: BuildConfig,
    rng: random.Random,
    now_epoch: int,
) -> tuple[list[tuple], list[int], list[str], list[int]]:
    post_start = now_epoch - 180 * 24 * 3600
    posts: list[tuple] = []
    post_owner = [0] * (cfg.posts + 1)
    post_topic = [""] * (cfg.posts + 1)
    post_created_epoch = [0] * (cfg.posts + 1)

    for post_id in range(1, cfg.posts + 1):
        user_id = rng.randint(1, cfg.users)
        topic = rng.choice(TOPICS)
        created_epoch = rand_epoch(rng, post_start, now_epoch)
        content = f"{rng.choice(POST_PHRASES)} {topic}. #{rng.choice(TOPIC_TAGS[topic])}"
        posts.append(
            (
                post_id,
                user_id,
                content,
                rng.choice(MEDIA_TYPES),
                topic,
                rng.choice(DEVICE_TYPES),
                timestamp_from_epoch(created_epoch),
            )
        )
        post_owner[post_id] = user_id
        post_topic[post_id] = topic
        post_created_epoch[post_id] = created_epoch

    return posts, post_owner, post_topic, post_created_epoch


def generate_tags() -> list[str]:
    tags = sorted({tag for tags in TOPIC_TAGS.values() for tag in tags})
    tags.extend(["breaking", "viral", "community", "trending", "analysis"])
    return sorted(set(tags))


def generate_post_tags(
    cfg: BuildConfig,
    rng: random.Random,
    tag_id_by_name: dict[str, int],
    post_topic: list[str],
) -> list[tuple]:
    rows: list[tuple] = []
    for post_id in range(1, cfg.posts + 1):
        topic = post_topic[post_id]
        topic_tags = TOPIC_TAGS[topic]
        k = rng.randint(1, 3)
        chosen = set(rng.sample(topic_tags, k=min(k, len(topic_tags))))
        if rng.random() < 0.30:
            chosen.add(rng.choice(("breaking", "viral", "community", "trending", "analysis")))
        for tag_name in chosen:
            rows.append((post_id, tag_id_by_name[tag_name]))
    return rows


def generate_likes(
    cfg: BuildConfig,
    rng: random.Random,
    now_epoch: int,
    post_owner: list[int],
    post_created_epoch: list[int],
) -> list[tuple]:
    likes: list[tuple] = []
    seen: set[tuple[int, int]] = set()

    while len(likes) < cfg.likes:
        post_id = rng.randint(1, cfg.posts)
        user_id = rng.randint(1, cfg.users)
        if post_owner[post_id] == user_id:
            continue
        pair = (user_id, post_id)
        if pair in seen:
            continue
        seen.add(pair)
        created_epoch = rand_epoch(rng, post_created_epoch[post_id], now_epoch)
        likes.append(
            (
                user_id,
                post_id,
                rng.choice(DEVICE_TYPES),
                timestamp_from_epoch(created_epoch),
            )
        )
    return likes


def generate_comments(
    cfg: BuildConfig,
    rng: random.Random,
    now_epoch: int,
    post_owner: list[int],
    post_topic: list[str],
    post_created_epoch: list[int],
) -> list[tuple]:
    comments: list[tuple] = []
    comments_by_post: dict[int, list[int]] = defaultdict(list)

    for comment_id in range(1, cfg.comments + 1):
        post_id = rng.randint(1, cfg.posts)
        user_id = rng.randint(1, cfg.users)
        if rng.random() < 0.25:
            user_id = post_owner[post_id]

        parent_comment_id = None
        existing_on_post = comments_by_post.get(post_id)
        if existing_on_post and rng.random() < 0.30:
            parent_comment_id = rng.choice(existing_on_post)

        topic = post_topic[post_id]
        comment_text = f"{rng.choice(COMMENT_PHRASES)} {topic}."
        created_epoch = rand_epoch(rng, post_created_epoch[post_id], now_epoch)

        comments.append(
            (
                comment_id,
                post_id,
                user_id,
                parent_comment_id,
                rng.choice(DEVICE_TYPES),
                comment_text,
                timestamp_from_epoch(created_epoch),
            )
        )
        comments_by_post[post_id].append(comment_id)

    return comments


def generate_login_activity(
    cfg: BuildConfig,
    rng: random.Random,
    now_epoch: int,
) -> list[tuple]:
    login_rows: list[tuple] = []
    login_start = now_epoch - 120 * 24 * 3600

    for user_id in range(1, cfg.users + 1):
        user_login_count = max(1, int(rng.gauss(cfg.logins_per_user, 2)))
        for _ in range(user_login_count):
            login_rows.append(
                (
                    user_id,
                    "login",
                    "session",
                    None,
                    rng.choice(DEVICE_TYPES),
                    timestamp_from_epoch(rand_epoch(rng, login_start, now_epoch)),
                )
            )
    return login_rows


def insert_many(conn: sqlite3.Connection, sql: str, rows: Sequence[tuple], label: str) -> None:
    start = timed(f"Inserting {label} ({len(rows)} rows)")
    with conn:
        for batch in chunked_rows(rows):
            conn.executemany(sql, batch)
    stop_timer(start, f"Inserted {label}")


def build_database(db_path: Path, cfg: BuildConfig, rebuild: bool) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists() and not rebuild:
        raise FileExistsError(
            f"Database {db_path} already exists. Use --rebuild to regenerate it."
        )
    if db_path.exists() and rebuild:
        db_path.unlink()

    rng = random.Random(cfg.seed)
    now_epoch = int(datetime.now(tz=timezone.utc).timestamp())

    start_all = time.perf_counter()
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")

    start = timed("Applying schema and triggers")
    conn.executescript(load_sql("schema.sql"))
    conn.executescript(load_sql("triggers.sql"))
    stop_timer(start, "Schema ready")

    users = generate_users(cfg, rng, now_epoch)
    insert_many(
        conn,
        """
        INSERT INTO users (user_id, username, email, city, bio, joined_at, is_verified)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        users,
        "users",
    )

    follows = generate_follows(cfg, rng, now_epoch)
    insert_many(
        conn,
        """
        INSERT INTO follows (follower_user_id, followee_user_id, source_device, followed_at)
        VALUES (?, ?, ?, ?)
        """,
        follows,
        "follows",
    )

    posts, post_owner, post_topic, post_created_epoch = generate_posts(cfg, rng, now_epoch)
    insert_many(
        conn,
        """
        INSERT INTO posts (post_id, user_id, content, media_type, topic, source_device, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        posts,
        "posts",
    )

    tags = generate_tags()
    tag_rows = [(idx + 1, tag) for idx, tag in enumerate(tags)]
    insert_many(conn, "INSERT INTO tags (tag_id, tag_name) VALUES (?, ?)", tag_rows, "tags")
    tag_id_by_name = {tag: idx + 1 for idx, tag in enumerate(tags)}

    post_tags = generate_post_tags(cfg, rng, tag_id_by_name, post_topic)
    insert_many(
        conn,
        "INSERT INTO post_tags (post_id, tag_id) VALUES (?, ?)",
        post_tags,
        "post_tags",
    )

    likes = generate_likes(cfg, rng, now_epoch, post_owner, post_created_epoch)
    insert_many(
        conn,
        """
        INSERT INTO likes (user_id, post_id, source_device, created_at)
        VALUES (?, ?, ?, ?)
        """,
        likes,
        "likes",
    )

    comments = generate_comments(cfg, rng, now_epoch, post_owner, post_topic, post_created_epoch)
    insert_many(
        conn,
        """
        INSERT INTO comments (
            comment_id,
            post_id,
            user_id,
            parent_comment_id,
            source_device,
            comment_text,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        comments,
        "comments",
    )

    login_activity = generate_login_activity(cfg, rng, now_epoch)
    insert_many(
        conn,
        """
        INSERT INTO user_activity (
            user_id,
            activity_type,
            entity_type,
            entity_id,
            device_type,
            activity_time
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        login_activity,
        "login activity",
    )

    start = timed("Applying indexes and views")
    conn.executescript(load_sql("indexes.sql"))
    conn.executescript(load_sql("views.sql"))
    conn.execute("ANALYZE;")
    stop_timer(start, "Indexes and views ready")

    print("\n[+] Final table counts")
    for table in (
        "users",
        "posts",
        "tags",
        "post_tags",
        "follows",
        "likes",
        "comments",
        "user_activity",
    ):
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"    - {table:14s}: {count}")

    conn.close()
    print(f"\n[+] Database build completed in {time.perf_counter() - start_all:.2f}s")
    print(f"[+] Database path: {db_path}")


def main() -> None:
    args = parse_args()
    cfg = BuildConfig(
        users=args.users,
        posts=args.posts,
        likes=args.likes,
        comments=args.comments,
        follows=args.follows,
        logins_per_user=args.logins_per_user,
        seed=args.seed,
    )
    build_database(args.db, cfg, args.rebuild)


if __name__ == "__main__":
    main()
