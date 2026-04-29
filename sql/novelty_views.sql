-- ============================================================
-- BoltDB — Novelty Features (novelty_views.sql)
-- Three views not found in any existing social media platform.
-- ============================================================

-- ============================================================
-- NOVELTY 1: Echo Chamber Coefficient (ECC)
-- ============================================================
-- Measures how concentrated a user's engagement is across topics.
-- Uses the Herfindahl-Hirschman Index (HHI), borrowed from
-- economics where it measures market monopoly. Here a score of
-- 1.0 = user only ever touches one topic (maximum echo chamber),
-- 0.0 would mean perfectly uniform engagement across all topics.
--
-- No social media platform today publicly exposes a filter-bubble
-- score to users, nor uses it to power a proactive diversity feed.
-- ============================================================
CREATE VIEW IF NOT EXISTS v_echo_chamber AS
WITH user_topic_engagement AS (
    -- Combine likes and comments into one engagement signal per topic
    SELECT l.user_id, p.topic, COUNT(*) AS cnt
    FROM likes l
    JOIN posts p ON p.post_id = l.post_id
    GROUP BY l.user_id, p.topic

    UNION ALL

    SELECT c.user_id, p.topic, COUNT(*) AS cnt
    FROM comments c
    JOIN posts p ON p.post_id = c.post_id
    GROUP BY c.user_id, p.topic
),
aggregated AS (
    SELECT user_id, topic, SUM(cnt) AS total
    FROM user_topic_engagement
    GROUP BY user_id, topic
),
user_grand_total AS (
    SELECT user_id, SUM(total) AS grand_total
    FROM aggregated
    GROUP BY user_id
),
topic_shares AS (
    SELECT
        a.user_id,
        a.topic,
        CAST(a.total AS REAL) / ugt.grand_total AS share
    FROM aggregated a
    JOIN user_grand_total ugt ON ugt.user_id = a.user_id
),
hhi AS (
    -- HHI = sum of squared market shares (topic shares here)
    SELECT
        user_id,
        SUM(share * share)     AS hhi_score,
        COUNT(DISTINCT topic)  AS topics_engaged
    FROM topic_shares
    GROUP BY user_id
)
SELECT
    u.user_id,
    u.username,
    ROUND(h.hhi_score, 4)                          AS echo_chamber_index,
    h.topics_engaged,
    ROUND((1.0 - h.hhi_score) * 100.0, 1)         AS diversity_score,
    CASE
        WHEN h.hhi_score >= 0.60 THEN 'Severe bubble'
        WHEN h.hhi_score >= 0.40 THEN 'Moderate bubble'
        WHEN h.hhi_score >= 0.20 THEN 'Mild bubble'
        ELSE 'Healthy diversity'
    END                                            AS bubble_severity,
    -- The single dominant topic for this user
    (
        SELECT ts2.topic
        FROM topic_shares ts2
        WHERE ts2.user_id = h.user_id
        ORDER BY ts2.share DESC
        LIMIT 1
    )                                              AS dominant_topic,
    -- What fraction of total engagement that top topic represents
    ROUND(
        (
            SELECT MAX(ts2.share)
            FROM topic_shares ts2
            WHERE ts2.user_id = h.user_id
        ) * 100.0, 1
    )                                              AS dominant_topic_pct
FROM users u
JOIN hhi h ON h.user_id = u.user_id;


-- ============================================================
-- NOVELTY 2: Conversation Quality Index (CQI)
-- ============================================================
-- Scores each post not on total engagement volume, but on the
-- richness of discourse it generated. Rewards:
--   • More unique commenters (diverse voices)
--   • Higher reply-to-comment ratio (deep threading, not just
--     top-level shouts)
--   • Author responsiveness (did the post creator reply back?)
--   • Spread of engagement over time (not a single spike)
--
-- No platform currently surfaces a "best discussion" feed ranked
-- by conversation depth rather than raw like/comment counts.
-- CQI makes it possible to build such a feed entirely in SQL.
-- ============================================================
CREATE VIEW IF NOT EXISTS v_conversation_quality AS
WITH comment_metrics AS (
    SELECT
        post_id,
        COUNT(*)                                               AS total_comments,
        COUNT(DISTINCT user_id)                               AS unique_commenters,
        -- Reply ratio: proportion of comments that are replies
        ROUND(
            CAST(
                COUNT(CASE WHEN parent_comment_id IS NOT NULL THEN 1 END)
            AS REAL) / NULLIF(COUNT(*), 0),
            4
        )                                                     AS reply_ratio,
        -- Commenter diversity: unique commenters / total comments
        -- 1.0 = every person commented once (perfectly diverse)
        -- < 0.5 = a few people dominating the thread
        ROUND(
            CAST(COUNT(DISTINCT user_id) AS REAL) /
            NULLIF(COUNT(*), 0),
            4
        )                                                     AS commenter_diversity,
        -- Time span of the discussion (hours) — longer = more sustained
        ROUND(
            (
                CAST(julianday(MAX(created_at)) AS REAL) -
                CAST(julianday(MIN(created_at)) AS REAL)
            ) * 24.0,
            2
        )                                                     AS discussion_span_hours
    FROM comments
    GROUP BY post_id
),
author_responsiveness AS (
    -- Does the post author participate in their own comment section?
    SELECT
        c.post_id,
        COUNT(CASE WHEN c.user_id = p.user_id THEN 1 END) AS author_reply_count
    FROM comments c
    JOIN posts p ON p.post_id = c.post_id
    GROUP BY c.post_id
)
SELECT
    p.post_id,
    u.username,
    p.topic,
    p.created_at,
    p.like_count,
    p.comment_count,
    COALESCE(cm.unique_commenters, 0)                      AS unique_commenters,
    COALESCE(cm.reply_ratio, 0)                            AS reply_ratio,
    COALESCE(cm.commenter_diversity, 0)                    AS commenter_diversity,
    COALESCE(cm.discussion_span_hours, 0)                  AS discussion_span_hours,
    COALESCE(ar.author_reply_count, 0)                     AS author_replies,
    -- CQI formula — all terms are bounded and additive:
    --   unique_commenters  × 3.0  (breadth of voices)
    --   reply_ratio        × 60.0 (depth of threading, 0-1 scaled up)
    --   commenter_diversity× 40.0 (discourse balance, 0-1 scaled up)
    --   author_reply_count × 4.0  (creator engagement)
    --   discussion_span_hours×0.5 (sustained interest, capped effect)
    --   like_count         × 0.05 (light signal — quality can exist
    --                              without viral likes)
    ROUND(
        COALESCE(cm.unique_commenters,  0) *  3.0  +
        COALESCE(cm.reply_ratio,        0) * 60.0  +
        COALESCE(cm.commenter_diversity,0) * 40.0  +
        COALESCE(ar.author_reply_count, 0) *  4.0  +
        MIN(COALESCE(cm.discussion_span_hours, 0), 72.0) * 0.5 +
        p.like_count * 0.05,
        2
    )                                                      AS cqi,
    DENSE_RANK() OVER (
        PARTITION BY p.topic
        ORDER BY (
            COALESCE(cm.unique_commenters,  0) *  3.0 +
            COALESCE(cm.reply_ratio,        0) * 60.0 +
            COALESCE(cm.commenter_diversity,0) * 40.0 +
            COALESCE(ar.author_reply_count, 0) *  4.0 +
            MIN(COALESCE(cm.discussion_span_hours, 0), 72.0) * 0.5 +
            p.like_count * 0.05
        ) DESC
    )                                                      AS cqi_rank_in_topic,
    DENSE_RANK() OVER (
        ORDER BY (
            COALESCE(cm.unique_commenters,  0) *  3.0 +
            COALESCE(cm.reply_ratio,        0) * 60.0 +
            COALESCE(cm.commenter_diversity,0) * 40.0 +
            COALESCE(ar.author_reply_count, 0) *  4.0 +
            MIN(COALESCE(cm.discussion_span_hours, 0), 72.0) * 0.5 +
            p.like_count * 0.05
        ) DESC
    )                                                      AS cqi_rank_global
FROM posts p
JOIN users u ON u.user_id = p.user_id
LEFT JOIN comment_metrics       cm ON cm.post_id = p.post_id
LEFT JOIN author_responsiveness ar ON ar.post_id = p.post_id;


-- ============================================================
-- NOVELTY 3: Social Reciprocity Score (SRS)
-- ============================================================
-- For every pair of users who have interacted, computes:
--   • How many likes A gave to B's posts
--   • How many likes B gave to A's posts
--   • An imbalance ratio (0 = perfectly mutual, 1 = fully one-sided)
--   • A human-readable relationship health label
--
-- No social media platform currently surfaces this information to
-- users. It enables two features that do not exist anywhere:
--   1. "Relationship health" panel — see who you neglect and
--      who neglects you, with no existing equivalent on any
--      major platform.
--   2. "Social debt" notifications — "You've liked 0 of
--      @alice's last 20 posts, but she's liked 18 of yours."
-- ============================================================
CREATE VIEW IF NOT EXISTS v_social_reciprocity AS
WITH pair_interactions AS (
    -- Normalise each like into a canonical (lower_id, higher_id) pair
    -- so each relationship appears exactly once
    SELECT
        CASE WHEN l.user_id < p.user_id THEN l.user_id ELSE p.user_id END AS user_a,
        CASE WHEN l.user_id < p.user_id THEN p.user_id ELSE l.user_id END AS user_b,
        l.user_id  AS liker,
        p.user_id  AS post_owner
    FROM likes l
    JOIN posts p ON p.post_id = l.post_id
    WHERE l.user_id <> p.user_id
),
pair_counts AS (
    SELECT
        user_a,
        user_b,
        SUM(CASE WHEN liker = user_a THEN 1 ELSE 0 END) AS a_likes_b,
        SUM(CASE WHEN liker = user_b THEN 1 ELSE 0 END) AS b_likes_a
    FROM pair_interactions
    GROUP BY user_a, user_b
    HAVING (a_likes_b + b_likes_a) > 0
)
SELECT
    ua.user_id                                             AS user_a_id,
    ua.username                                            AS user_a,
    ub.user_id                                             AS user_b_id,
    ub.username                                            AS user_b,
    pc.a_likes_b,
    pc.b_likes_a,
    (pc.a_likes_b + pc.b_likes_a)                         AS total_interactions,
    -- Imbalance ratio: 0 = perfectly mutual, 1 = completely one-sided
    ROUND(
        CAST(ABS(pc.a_likes_b - pc.b_likes_a) AS REAL) /
        NULLIF(pc.a_likes_b + pc.b_likes_a, 0),
        4
    )                                                      AS imbalance_ratio,
    CASE
        WHEN pc.a_likes_b = pc.b_likes_a              THEN 'Perfectly mutual'
        WHEN CAST(ABS(pc.a_likes_b - pc.b_likes_a) AS REAL) /
             (pc.a_likes_b + pc.b_likes_a) < 0.20    THEN 'Balanced'
        WHEN CAST(ABS(pc.a_likes_b - pc.b_likes_a) AS REAL) /
             (pc.a_likes_b + pc.b_likes_a) < 0.50    THEN 'Slightly one-sided'
        WHEN CAST(ABS(pc.a_likes_b - pc.b_likes_a) AS REAL) /
             (pc.a_likes_b + pc.b_likes_a) < 0.80    THEN 'One-sided'
        ELSE                                              'Parasocial'
    END                                                    AS relationship_health,
    -- Who is the more engaged party?
    CASE
        WHEN pc.a_likes_b > pc.b_likes_a THEN ua.username
        WHEN pc.b_likes_a > pc.a_likes_b THEN ub.username
        ELSE 'Equal'
    END                                                    AS more_engaged_party,
    -- Social debt: positive = user_a owes user_b, negative = vice versa
    (pc.b_likes_a - pc.a_likes_b)                         AS social_debt_of_a
FROM pair_counts pc
JOIN users ua ON ua.user_id = pc.user_a
JOIN users ub ON ub.user_id = pc.user_b;
