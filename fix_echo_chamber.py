#!/usr/bin/env python3
"""Skew certain users' likes to create realistic echo chamber patterns.

After running, the Echo Chamber Leaderboard will show a nice descending
gradient from severe bubbles (~0.8 HHI) to mild bubbles (~0.2 HHI).
"""

import random
import sqlite3
from pathlib import Path

DB = Path(__file__).resolve().parent / "data" / "social_media.db"
random.seed(42)

conn = sqlite3.connect(str(DB))

# Get all topics
topics = [r[0] for r in conn.execute("SELECT DISTINCT topic FROM posts ORDER BY topic").fetchall()]
print(f"Topics: {topics}")

# Get posts grouped by topic for quick lookup
posts_by_topic: dict[str, list[int]] = {}
for topic in topics:
    posts_by_topic[topic] = [
        r[0] for r in conn.execute(
            "SELECT post_id FROM posts WHERE topic = ? ORDER BY RANDOM()", (topic,)
        ).fetchall()
    ]

# Define 15 "bubble users" with varying degrees of concentration
# Each entry: (user_id, dominant_topic, concentration %)
# High concentration = high HHI = severe echo chamber
bubble_profiles = [
    # Severe bubbles (HHI ~0.70-0.85)
    (2480, "gaming",     0.92),   # Almost only gaming
    (2387, "music",      0.88),   # Music obsessed
    (1200, "sports",     0.85),   # Sports fanatic
    (1605, "technology", 0.80),   # Tech nerd
    (1336, "movies",     0.75),   # Movie buff
    # Moderate bubbles (HHI ~0.40-0.60)
    (2776, "food",       0.68),   # Foodie
    (2623, "health",     0.62),   # Health focused
    (1534, "finance",    0.55),   # Finance bro
    (2015, "education",  0.50),   # Education oriented
    (2386, "travel",     0.45),   # Travel lover
    # Mild bubbles (HHI ~0.20-0.35) — two dominant topics
    (1900, "gaming",     0.38),   # Gaming + music
    (2100, "sports",     0.35),   # Sports + movies
    (1750, "technology", 0.30),   # Tech + education
    (2250, "food",       0.25),   # Food + travel
    (1400, "movies",     0.22),   # Movies + music
]

LIKES_PER_USER = 40  # How many likes each bubble user will have

for user_id, dominant_topic, concentration in bubble_profiles:
    # Check user exists
    user = conn.execute("SELECT username FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if not user:
        print(f"  [skip] user_id={user_id} not found")
        continue

    # Delete existing likes for this user
    old_count = conn.execute("SELECT COUNT(*) FROM likes WHERE user_id = ?", (user_id,)).fetchone()[0]
    conn.execute("DELETE FROM likes WHERE user_id = ?", (user_id,))

    # Build new like distribution
    n_dominant = int(LIKES_PER_USER * concentration)
    n_other = LIKES_PER_USER - n_dominant
    other_topics = [t for t in topics if t != dominant_topic]

    # Pick posts for dominant topic
    dominant_posts = random.sample(
        posts_by_topic[dominant_topic],
        min(n_dominant, len(posts_by_topic[dominant_topic]))
    )

    # Pick posts for other topics (spread evenly)
    other_posts = []
    for i in range(n_other):
        t = other_topics[i % len(other_topics)]
        available = [p for p in posts_by_topic[t] if p not in other_posts]
        if available:
            other_posts.append(random.choice(available))

    all_posts = dominant_posts + other_posts

    # Insert new likes
    devices = ["web", "ios", "android"]
    inserted = 0
    for post_id in all_posts:
        try:
            conn.execute(
                "INSERT INTO likes (user_id, post_id, source_device, created_at) "
                "VALUES (?, ?, ?, datetime('now', '-' || ? || ' days'))",
                (user_id, post_id, random.choice(devices), random.randint(1, 60))
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # Duplicate like, skip

    print(f"  {user[0]:15s} (id={user_id}): {dominant_topic:12s} @ {concentration:.0%} "
          f"— was {old_count} likes, now {inserted}")

# Also skew comments for the top 5 most-bubbled users to reinforce the pattern
print("\nSkewing comments for top 5 bubble users...")
for user_id, dominant_topic, concentration in bubble_profiles[:5]:
    user = conn.execute("SELECT username FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if not user:
        continue

    # Delete existing comments
    old_comments = conn.execute("SELECT COUNT(*) FROM comments WHERE user_id = ?", (user_id,)).fetchone()[0]
    conn.execute("DELETE FROM comments WHERE user_id = ?", (user_id,))

    # Add comments mostly on dominant topic posts
    n_comments = 20
    n_dominant_comments = int(n_comments * concentration)
    dominant_posts = random.sample(
        posts_by_topic[dominant_topic],
        min(n_dominant_comments, len(posts_by_topic[dominant_topic]))
    )
    other_topics = [t for t in topics if t != dominant_topic]
    other_posts = []
    for i in range(n_comments - n_dominant_comments):
        t = other_topics[i % len(other_topics)]
        available = posts_by_topic[t]
        if available:
            other_posts.append(random.choice(available))

    comment_texts = [
        "Great point about this topic!", "Totally agree with this.",
        "This is so interesting!", "I've been thinking the same thing.",
        "Love this content!", "Can you share more about this?",
        "This changed my perspective.", "Fascinating analysis.",
        "This is exactly what I was looking for.", "Well said!",
    ]

    inserted = 0
    for post_id in (dominant_posts + other_posts):
        try:
            conn.execute(
                "INSERT INTO comments (post_id, user_id, source_device, comment_text, created_at) "
                "VALUES (?, ?, ?, ?, datetime('now', '-' || ? || ' days'))",
                (post_id, user_id, random.choice(devices),
                 random.choice(comment_texts), random.randint(1, 60))
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    print(f"  {user[0]:15s}: was {old_comments} comments, now {inserted}")

conn.commit()

# Update like_count and comment_count on posts
print("\nRecalculating post counters...")
conn.execute("""
    UPDATE posts SET like_count = (
        SELECT COUNT(*) FROM likes WHERE likes.post_id = posts.post_id
    )
""")
conn.execute("""
    UPDATE posts SET comment_count = (
        SELECT COUNT(*) FROM comments WHERE comments.post_id = posts.post_id
    )
""")
conn.commit()

# Verify: check echo chamber scores
print("\nVerifying echo chamber scores:")
rows = conn.execute("""
    SELECT username, echo_chamber_index, diversity_score, bubble_severity, dominant_topic
    FROM v_echo_chamber
    ORDER BY echo_chamber_index DESC
    LIMIT 15
""").fetchall()
for r in rows:
    bar = "█" * int(r[1] * 40)
    print(f"  {r[0]:15s}  HHI={r[1]:.4f}  div={r[2]:5.1f}%  {r[3]:20s}  {r[4]:12s}  {bar}")

conn.close()
print("\n✅ Done! Refresh the Novelty Lab in the dashboard.")
