-- 1) Top influencers ranked by engagement and follower reach.
WITH ranked AS (
    SELECT
        user_id,
        username,
        follower_count,
        post_count,
        likes_received,
        comments_received,
        influence_score,
        DENSE_RANK() OVER (ORDER BY influence_score DESC) AS influence_rank
    FROM v_user_influence
)
SELECT *
FROM ranked
ORDER BY influence_rank
LIMIT 20;

-- 2) Trending topics in the last 7 days.
WITH recent AS (
    SELECT post_id, topic, like_count, comment_count
    FROM posts
    WHERE created_at >= datetime('now', '-7 days')
)
SELECT
    topic,
    COUNT(*) AS posts_in_window,
    SUM(like_count) AS likes_in_window,
    SUM(comment_count) AS comments_in_window,
    SUM(like_count + 2 * comment_count) AS trend_score
FROM recent
GROUP BY topic
ORDER BY trend_score DESC, posts_in_window DESC
LIMIT 10;

-- 3) Trending tags in the last 14 days.
SELECT
    t.tag_name,
    COUNT(DISTINCT p.post_id) AS tagged_posts,
    SUM(p.like_count) AS likes,
    SUM(p.comment_count) AS comments,
    SUM(p.like_count + 2 * p.comment_count) AS weighted_score
FROM posts p
JOIN post_tags pt ON pt.post_id = p.post_id
JOIN tags t ON t.tag_id = pt.tag_id
WHERE p.created_at >= datetime('now', '-14 days')
GROUP BY t.tag_name
ORDER BY weighted_score DESC
LIMIT 15;

-- 4) Recommend posts to user :uid from similar users.
WITH liked_by_target AS (
    SELECT post_id
    FROM likes
    WHERE user_id = :uid
),
similar_users AS (
    SELECT l2.user_id, COUNT(*) AS overlap
    FROM likes l1
    JOIN likes l2 ON l1.post_id = l2.post_id
    WHERE l1.user_id = :uid
      AND l2.user_id <> :uid
    GROUP BY l2.user_id
    ORDER BY overlap DESC
    LIMIT 50
),
candidates AS (
    SELECT l.post_id, COUNT(*) AS support
    FROM likes l
    JOIN similar_users su ON su.user_id = l.user_id
    WHERE l.post_id NOT IN (SELECT post_id FROM liked_by_target)
    GROUP BY l.post_id
)
SELECT
    p.post_id,
    p.topic,
    p.like_count,
    p.comment_count,
    c.support,
    (c.support + p.like_count * 0.10 + p.comment_count * 0.20) AS recommendation_score
FROM candidates c
JOIN posts p ON p.post_id = c.post_id
ORDER BY recommendation_score DESC
LIMIT 20;

-- 5) Recursive comment tree for post :pid.
WITH RECURSIVE thread AS (
    SELECT
        c.comment_id,
        c.post_id,
        c.user_id,
        c.parent_comment_id,
        c.comment_text,
        c.created_at,
        0 AS depth,
        printf('%08d', c.comment_id) AS path
    FROM comments c
    WHERE c.post_id = :pid
      AND c.parent_comment_id IS NULL

    UNION ALL

    SELECT
        c.comment_id,
        c.post_id,
        c.user_id,
        c.parent_comment_id,
        c.comment_text,
        c.created_at,
        t.depth + 1,
        t.path || '>' || printf('%08d', c.comment_id)
    FROM comments c
    JOIN thread t ON t.comment_id = c.parent_comment_id
)
SELECT
    th.comment_id,
    u.username,
    th.depth,
    th.comment_text,
    th.created_at
FROM thread th
JOIN users u ON u.user_id = th.user_id
ORDER BY th.path
LIMIT 100;

-- 6) Activity spike detection with 24-hour rolling average.
WITH hourly AS (
    SELECT
        strftime('%Y-%m-%d %H:00:00', activity_time) AS hour_bucket,
        COUNT(*) AS total_events
    FROM user_activity
    GROUP BY strftime('%Y-%m-%d %H:00:00', activity_time)
),
series AS (
    SELECT
        hour_bucket,
        total_events,
        AVG(total_events) OVER (
            ORDER BY hour_bucket
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_24h
    FROM hourly
)
SELECT
    hour_bucket,
    total_events,
    ROUND(rolling_avg_24h, 2) AS rolling_avg_24h,
    ROUND(total_events / NULLIF(rolling_avg_24h, 0), 2) AS spike_ratio
FROM series
WHERE rolling_avg_24h IS NOT NULL
ORDER BY spike_ratio DESC, hour_bucket DESC
LIMIT 25;

-- 7) Feed query: posts from followed users not liked by :uid yet.
SELECT
    p.post_id,
    u.username,
    p.topic,
    p.created_at,
    p.like_count,
    p.comment_count
FROM follows f
JOIN posts p ON p.user_id = f.followee_user_id
JOIN users u ON u.user_id = p.user_id
LEFT JOIN likes l ON l.user_id = f.follower_user_id AND l.post_id = p.post_id
WHERE f.follower_user_id = :uid
  AND l.like_id IS NULL
ORDER BY p.created_at DESC
LIMIT 50;

-- 8) Cohort retention (first 6 months).
WITH cohort AS (
    SELECT user_id, strftime('%Y-%m', joined_at) AS cohort_month
    FROM users
),
activity_month AS (
    SELECT user_id, strftime('%Y-%m', activity_time) AS active_month
    FROM user_activity
    GROUP BY user_id, strftime('%Y-%m', activity_time)
),
joined AS (
    SELECT
        c.cohort_month,
        am.active_month,
        (
            (CAST(substr(am.active_month, 1, 4) AS INTEGER) - CAST(substr(c.cohort_month, 1, 4) AS INTEGER)) * 12 +
            (CAST(substr(am.active_month, 6, 2) AS INTEGER) - CAST(substr(c.cohort_month, 6, 2) AS INTEGER))
        ) AS month_number,
        COUNT(DISTINCT am.user_id) AS active_users
    FROM cohort c
    JOIN activity_month am ON am.user_id = c.user_id
    WHERE am.active_month >= c.cohort_month
    GROUP BY c.cohort_month, am.active_month
),
cohort_size AS (
    SELECT strftime('%Y-%m', joined_at) AS cohort_month, COUNT(*) AS users_in_cohort
    FROM users
    GROUP BY strftime('%Y-%m', joined_at)
)
SELECT
    j.cohort_month,
    j.month_number,
    cs.users_in_cohort,
    j.active_users,
    ROUND(100.0 * j.active_users / cs.users_in_cohort, 2) AS retention_percent
FROM joined j
JOIN cohort_size cs ON cs.cohort_month = j.cohort_month
WHERE j.month_number BETWEEN 0 AND 6
ORDER BY j.cohort_month, j.month_number;

-- 9) Monthly top creators (window function).
WITH monthly_user_stats AS (
    SELECT
        strftime('%Y-%m', p.created_at) AS month,
        p.user_id,
        COUNT(*) AS posts,
        SUM(p.like_count) AS likes,
        SUM(p.comment_count) AS comments,
        SUM(p.like_count + 2 * p.comment_count) AS engagement
    FROM posts p
    GROUP BY strftime('%Y-%m', p.created_at), p.user_id
),
ranked AS (
    SELECT
        mus.*,
        DENSE_RANK() OVER (
            PARTITION BY month
            ORDER BY engagement DESC
        ) AS monthly_rank
    FROM monthly_user_stats mus
)
SELECT
    r.month,
    r.user_id,
    u.username,
    r.posts,
    r.likes,
    r.comments,
    r.engagement,
    r.monthly_rank
FROM ranked r
JOIN users u ON u.user_id = r.user_id
WHERE r.monthly_rank <= 5
ORDER BY r.month DESC, r.monthly_rank;

-- 10) Query plan inspection for join-heavy analytics.
EXPLAIN QUERY PLAN
SELECT
    p.topic,
    COUNT(DISTINCT p.post_id) AS posts,
    COUNT(DISTINCT l.like_id) AS likes,
    COUNT(DISTINCT c.comment_id) AS comments,
    COUNT(DISTINCT f.follower_user_id) AS audience
FROM posts p
LEFT JOIN likes l ON l.post_id = p.post_id
LEFT JOIN comments c ON c.post_id = p.post_id
LEFT JOIN follows f ON f.followee_user_id = p.user_id
WHERE p.created_at >= datetime('now', '-30 days')
GROUP BY p.topic
ORDER BY likes + comments DESC;
