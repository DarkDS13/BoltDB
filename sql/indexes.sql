CREATE INDEX IF NOT EXISTS idx_users_joined_at ON users(joined_at);
CREATE INDEX IF NOT EXISTS idx_users_verified_city ON users(is_verified, city);

CREATE INDEX IF NOT EXISTS idx_posts_user_time ON posts(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_topic_time ON posts(topic, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_post_tags_tag ON post_tags(tag_id, post_id);
CREATE INDEX IF NOT EXISTS idx_post_tags_post ON post_tags(post_id, tag_id);

CREATE INDEX IF NOT EXISTS idx_likes_post_time ON likes(post_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_likes_user_time ON likes(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_comments_post_time ON comments(post_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_comments_user_time ON comments(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_comment_id);

CREATE INDEX IF NOT EXISTS idx_follows_followee ON follows(followee_user_id, followed_at DESC);
CREATE INDEX IF NOT EXISTS idx_follows_follower ON follows(follower_user_id, followed_at DESC);

CREATE INDEX IF NOT EXISTS idx_activity_user_time ON user_activity(user_id, activity_time DESC);
CREATE INDEX IF NOT EXISTS idx_activity_type_time ON user_activity(activity_type, activity_time DESC);
CREATE INDEX IF NOT EXISTS idx_activity_time ON user_activity(activity_time DESC);
