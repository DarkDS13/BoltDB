PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    city TEXT NOT NULL,
    bio TEXT,
    joined_at TEXT NOT NULL,
    is_verified INTEGER NOT NULL DEFAULT 0 CHECK (is_verified IN (0, 1))
);

CREATE TABLE IF NOT EXISTS posts (
    post_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    media_type TEXT NOT NULL CHECK (media_type IN ('text', 'image', 'video', 'link')),
    topic TEXT NOT NULL,
    source_device TEXT NOT NULL CHECK (source_device IN ('web', 'ios', 'android')),
    created_at TEXT NOT NULL,
    like_count INTEGER NOT NULL DEFAULT 0 CHECK (like_count >= 0),
    comment_count INTEGER NOT NULL DEFAULT 0 CHECK (comment_count >= 0),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tags (
    tag_id INTEGER PRIMARY KEY,
    tag_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS post_tags (
    post_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    PRIMARY KEY (post_id, tag_id),
    FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS follows (
    follower_user_id INTEGER NOT NULL,
    followee_user_id INTEGER NOT NULL,
    source_device TEXT NOT NULL CHECK (source_device IN ('web', 'ios', 'android')),
    followed_at TEXT NOT NULL,
    PRIMARY KEY (follower_user_id, followee_user_id),
    FOREIGN KEY (follower_user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (followee_user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    CHECK (follower_user_id <> followee_user_id)
);

CREATE TABLE IF NOT EXISTS likes (
    like_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    post_id INTEGER NOT NULL,
    source_device TEXT NOT NULL CHECK (source_device IN ('web', 'ios', 'android')),
    created_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE,
    UNIQUE (user_id, post_id)
);

CREATE TABLE IF NOT EXISTS comments (
    comment_id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    parent_comment_id INTEGER,
    source_device TEXT NOT NULL CHECK (source_device IN ('web', 'ios', 'android')),
    comment_text TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (parent_comment_id) REFERENCES comments(comment_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS user_activity (
    activity_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    activity_type TEXT NOT NULL CHECK (activity_type IN ('login', 'post', 'like', 'comment', 'follow')),
    entity_type TEXT NOT NULL CHECK (entity_type IN ('session', 'post', 'like', 'comment', 'follow')),
    entity_id INTEGER,
    device_type TEXT NOT NULL CHECK (device_type IN ('web', 'ios', 'android')),
    activity_time TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);
