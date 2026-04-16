CREATE VIEW IF NOT EXISTS v_post_engagement AS
SELECT
    p.post_id,
    p.user_id,
    u.username,
    p.topic,
    p.created_at,
    p.like_count,
    p.comment_count,
    (p.like_count + 2 * p.comment_count) AS engagement_score,
    DENSE_RANK() OVER (
        PARTITION BY p.topic
        ORDER BY (p.like_count + 2 * p.comment_count) DESC, p.created_at DESC
    ) AS topic_rank
FROM posts AS p
JOIN users AS u ON u.user_id = p.user_id;

CREATE VIEW IF NOT EXISTS v_user_influence AS
WITH follower_counts AS (
    SELECT followee_user_id AS user_id, COUNT(*) AS follower_count
    FROM follows
    GROUP BY followee_user_id
),
post_totals AS (
    SELECT
        user_id,
        COUNT(*) AS post_count,
        COALESCE(SUM(like_count), 0) AS likes_received,
        COALESCE(SUM(comment_count), 0) AS comments_received
    FROM posts
    GROUP BY user_id
)
SELECT
    u.user_id,
    u.username,
    COALESCE(fc.follower_count, 0) AS follower_count,
    COALESCE(pt.post_count, 0) AS post_count,
    COALESCE(pt.likes_received, 0) AS likes_received,
    COALESCE(pt.comments_received, 0) AS comments_received,
    (
        COALESCE(fc.follower_count, 0) * 3 +
        COALESCE(pt.likes_received, 0) +
        COALESCE(pt.comments_received, 0) * 2
    ) AS influence_score
FROM users u
LEFT JOIN follower_counts fc ON fc.user_id = u.user_id
LEFT JOIN post_totals pt ON pt.user_id = u.user_id;

CREATE VIEW IF NOT EXISTS v_daily_platform_activity AS
SELECT
    date(activity_time) AS activity_date,
    activity_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS unique_users
FROM user_activity
GROUP BY date(activity_time), activity_type;
