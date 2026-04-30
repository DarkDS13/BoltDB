#!/usr/bin/env python3
"""Inject artificial social debt with organic randomization."""

import sqlite3
import random
from pathlib import Path

DB = Path(__file__).resolve().parent / "data" / "social_media.db"
conn = sqlite3.connect(str(DB))

topics = [r[0] for r in conn.execute("SELECT DISTINCT topic FROM posts").fetchall()]

for user_id in range(1, 16):
    # Ensure user has enough posts
    current_posts = conn.execute("SELECT COUNT(*) FROM posts WHERE user_id = ?", (user_id,)).fetchone()[0]
    if current_posts < 50:
        for _ in range(50 - current_posts):
            conn.execute("INSERT INTO posts (user_id, topic, created_at, content, media_type, source_device) VALUES (?, ?, datetime('now', '-10 days'), 'Synthetic post', 'text', 'web')", (user_id, random.choice(topics)))
    conn.commit()

    user_posts = [r[0] for r in conn.execute("SELECT post_id FROM posts WHERE user_id = ?", (user_id,)).fetchall()]
    
    others = [r[0] for r in conn.execute("SELECT user_id FROM users WHERE user_id > 20 ORDER BY RANDOM() LIMIT 15").fetchall()]
    
    fans = others[0:3]
    idols = others[3:5]
    mutuals = others[5:9]
    one_sided = others[9:12]
    
    # 1. Super Fans (Parasocial)
    for fan in fans:
        likes_to_give = random.randint(7, 30)
        for post_id in random.sample(user_posts, min(likes_to_give, len(user_posts))):
            try:
                conn.execute("INSERT INTO likes (user_id, post_id, source_device, created_at) VALUES (?, ?, 'web', datetime('now', '-1 days'))", (fan, post_id))
            except sqlite3.IntegrityError: pass
            
    # 2. Idols (Parasocial)
    for idol in idols:
        current_idol_posts = conn.execute("SELECT COUNT(*) FROM posts WHERE user_id = ?", (idol,)).fetchone()[0]
        if current_idol_posts < 40:
            for _ in range(40 - current_idol_posts):
                conn.execute("INSERT INTO posts (user_id, topic, created_at, content, media_type, source_device) VALUES (?, ?, datetime('now', '-10 days'), 'Synthetic post', 'text', 'ios')", (idol, random.choice(topics)))
            conn.commit()
            
        idol_posts = [r[0] for r in conn.execute("SELECT post_id FROM posts WHERE user_id = ?", (idol,)).fetchall()]
        likes_to_give = random.randint(8, 35)
        for post_id in random.sample(idol_posts, min(likes_to_give, len(idol_posts))):
            try:
                conn.execute("INSERT INTO likes (user_id, post_id, source_device, created_at) VALUES (?, ?, 'ios', datetime('now', '-2 days'))", (user_id, post_id))
            except sqlite3.IntegrityError: pass
            
    # 3. Perfectly Mutual
    for friend in mutuals:
        current_friend_posts = conn.execute("SELECT COUNT(*) FROM posts WHERE user_id = ?", (friend,)).fetchone()[0]
        if current_friend_posts < 30:
            for _ in range(30 - current_friend_posts):
                conn.execute("INSERT INTO posts (user_id, topic, created_at, content, media_type, source_device) VALUES (?, ?, datetime('now', '-10 days'), 'Synthetic post', 'text', 'android')", (friend, random.choice(topics)))
            conn.commit()
            
        friend_posts = [r[0] for r in conn.execute("SELECT post_id FROM posts WHERE user_id = ?", (friend,)).fetchall()]
        
        mutual_likes = random.randint(12, 25)
        
        for post_id in random.sample(friend_posts, min(mutual_likes, len(friend_posts))):
            try:
                conn.execute("INSERT INTO likes (user_id, post_id, source_device, created_at) VALUES (?, ?, 'web', datetime('now', '-3 days'))", (user_id, post_id))
            except sqlite3.IntegrityError: pass
        for post_id in random.sample(user_posts, min(mutual_likes, len(user_posts))):
            try:
                conn.execute("INSERT INTO likes (user_id, post_id, source_device, created_at) VALUES (?, ?, 'android', datetime('now', '-3 days'))", (friend, post_id))
            except sqlite3.IntegrityError: pass
            
    # 4. Slightly One-Sided
    for friend in one_sided:
        current_friend_posts = conn.execute("SELECT COUNT(*) FROM posts WHERE user_id = ?", (friend,)).fetchone()[0]
        if current_friend_posts < 30:
            for _ in range(30 - current_friend_posts):
                conn.execute("INSERT INTO posts (user_id, topic, created_at, content, media_type, source_device) VALUES (?, ?, datetime('now', '-10 days'), 'Synthetic post', 'text', 'web')", (friend, random.choice(topics)))
            conn.commit()
            
        friend_posts = [r[0] for r in conn.execute("SELECT post_id FROM posts WHERE user_id = ?", (friend,)).fetchall()]
        
        u1_likes = random.randint(10, 20)
        f_likes = random.randint(3, 7)
        
        for post_id in random.sample(friend_posts, min(u1_likes, len(friend_posts))):
            try:
                conn.execute("INSERT INTO likes (user_id, post_id, source_device, created_at) VALUES (?, ?, 'web', datetime('now', '-4 days'))", (user_id, post_id))
            except sqlite3.IntegrityError: pass
        for post_id in random.sample(user_posts, min(f_likes, len(user_posts))):
            try:
                conn.execute("INSERT INTO likes (user_id, post_id, source_device, created_at) VALUES (?, ?, 'ios', datetime('now', '-4 days'))", (friend, post_id))
            except sqlite3.IntegrityError: pass

    print(f"Generated organic social debt for User {user_id}")

conn.commit()

# Recalculate post like counts
conn.execute("UPDATE posts SET like_count = (SELECT COUNT(*) FROM likes WHERE likes.post_id = posts.post_id)")
conn.commit()

print("\n✅ Done! Fixed uniformity.")
conn.close()
