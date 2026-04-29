-- ============================================================
-- BoltDB — Novelty Analytical Queries (novelty_queries.sql)
-- Queries that power the three novel user-facing features.
-- Requires: novelty_views.sql to be executed first.
-- ============================================================


-- ============================================================
-- NOVELTY 1 QUERIES — Echo Chamber Coefficient (ECC)
-- ============================================================

-- Q-N1a: Platform-wide echo chamber leaderboard.
-- Shows the most "bubbled" users — useful for moderation/research.
SELECT
    username,
    echo_chamber_index,
    diversity_score,
    topics_engaged,
    bubble_severity,
    dominant_topic,
    dominant_topic_pct
FROM v_echo_chamber
ORDER BY echo_chamber_index DESC
LIMIT 50;


-- Q-N1b: Diversity-nudge feed for user :uid.
-- Surfaces high-quality posts from topics the user almost never
-- engages with. This is the feed no existing platform provides:
-- deliberately diverse content, not algorithmically reinforcing
-- existing preferences.
WITH user_top2_topics AS (
    -- The two topics that dominate this user's engagement
    SELECT p.topic
    FROM likes l
    JOIN posts p ON p.post_id = l.post_id
    WHERE l.user_id = :uid
    GROUP BY p.topic
    ORDER BY COUNT(*) DESC
    LIMIT 2
),
already_interacted AS (
    SELECT post_id FROM likes    WHERE user_id = :uid
    UNION
    SELECT post_id FROM comments WHERE user_id = :uid
)
SELECT
    p.post_id,
    u.username,
    p.topic,
    p.created_at,
    p.like_count,
    p.comment_count,
    (p.like_count + 2 * p.comment_count)   AS engagement_score,
    vq.cqi                                 AS conversation_quality,
    'diversity_nudge'                       AS feed_source
FROM posts p
JOIN users u  ON u.user_id  = p.user_id
LEFT JOIN v_conversation_quality vq ON vq.post_id = p.post_id
WHERE p.topic NOT IN (SELECT topic FROM user_top2_topics)
  AND p.post_id NOT IN (SELECT post_id FROM already_interacted)
  AND p.created_at >= datetime('now', '-30 days')
ORDER BY
    -- Blend engagement + quality so the out-of-bubble content is
    -- still good, not random junk
    (p.like_count + 2 * p.comment_count) * 0.6 +
    COALESCE(vq.cqi, 0) * 0.4
    DESC
LIMIT 20;


-- Q-N1c: Per-topic breakdown of a single user's engagement.
-- Powers the "bubble breakdown" profile card:
-- "You spent 68 % of your engagement on Tech this month."
WITH user_topic_engagement AS (
    SELECT p.topic, COUNT(*) AS cnt
    FROM likes l JOIN posts p ON p.post_id = l.post_id
    WHERE l.user_id = :uid
    GROUP BY p.topic
    UNION ALL
    SELECT p.topic, COUNT(*) AS cnt
    FROM comments c JOIN posts p ON p.post_id = c.post_id
    WHERE c.user_id = :uid
    GROUP BY p.topic
),
aggregated AS (
    SELECT topic, SUM(cnt) AS total
    FROM user_topic_engagement
    GROUP BY topic
),
grand AS (
    SELECT SUM(total) AS g FROM aggregated
)
SELECT
    a.topic,
    a.total                                              AS interactions,
    ROUND(100.0 * a.total / g.g, 1)                    AS pct_of_total,
    -- How does this user's proportion compare to the platform average?
    ROUND(100.0 * a.total / g.g, 1) -
    ROUND(100.0 * (
        SELECT SUM(cnt2) FROM (
            SELECT COUNT(*) AS cnt2 FROM likes l2
            JOIN posts p2 ON p2.post_id = l2.post_id
            WHERE p2.topic = a.topic
        )
    ) / (SELECT SUM(total2) FROM (
            SELECT COUNT(*) AS total2 FROM likes
    )), 1)                                              AS vs_platform_avg_ppts
FROM aggregated a, grand g
ORDER BY a.total DESC;


-- ============================================================
-- NOVELTY 2 QUERIES — Conversation Quality Index (CQI)
-- ============================================================

-- Q-N2a: "Best discussions" global feed — the feed no platform
-- currently offers. Shows posts ranked by discourse quality, not
-- popularity. A 50-like post with a 30-person threaded debate
-- ranks above a 500-like post with 300 "lol" comments.
SELECT
    post_id,
    username,
    topic,
    like_count,
    comment_count,
    unique_commenters,
    ROUND(reply_ratio * 100, 1)    AS reply_pct,
    ROUND(commenter_diversity, 2)  AS commenter_diversity,
    author_replies,
    cqi,
    cqi_rank_global
FROM v_conversation_quality
WHERE comment_count >= 3           -- Require some baseline discussion
  AND created_at >= datetime('now', '-14 days')
ORDER BY cqi_rank_global
LIMIT 25;


-- Q-N2b: Best-discussion feed per topic — "Best of [topic]" tab.
SELECT
    post_id,
    username,
    topic,
    like_count,
    comment_count,
    unique_commenters,
    ROUND(reply_ratio * 100, 1)   AS reply_pct,
    author_replies,
    cqi,
    cqi_rank_in_topic
FROM v_conversation_quality
WHERE topic = :topic
  AND comment_count >= 2
ORDER BY cqi_rank_in_topic
LIMIT 20;


-- Q-N2c: "Hall of Fame" posts — all-time highest-CQI discussions.
-- Could power a weekly digest email: "Best conversations this week."
SELECT
    post_id,
    username,
    topic,
    created_at,
    like_count,
    comment_count,
    unique_commenters,
    ROUND(reply_ratio * 100, 1)          AS reply_pct,
    ROUND(discussion_span_hours, 1)      AS span_hours,
    author_replies,
    cqi,
    -- Classify the conversation type
    CASE
        WHEN reply_ratio > 0.6
         AND commenter_diversity > 0.7   THEN 'Rich debate'
        WHEN unique_commenters > 20      THEN 'Broad community response'
        WHEN author_replies > 5          THEN 'Creator-led discussion'
        WHEN discussion_span_hours > 48  THEN 'Slow-burn conversation'
        ELSE 'Standard discussion'
    END                                  AS conversation_type
FROM v_conversation_quality
WHERE comment_count >= 5
ORDER BY cqi DESC
LIMIT 20;


-- ============================================================
-- NOVELTY 3 QUERIES — Social Reciprocity Score (SRS)
-- ============================================================

-- Q-N3a: "Relationship health" panel for user :uid.
-- Shows every person :uid has interacted with, ranked by
-- relationship imbalance. The most one-sided relationships appear
-- first — prompting the user to either engage more or prune.
SELECT
    CASE WHEN user_a_id = :uid THEN user_b    ELSE user_a    END AS other_user,
    CASE WHEN user_a_id = :uid THEN a_likes_b ELSE b_likes_a END AS i_gave,
    CASE WHEN user_a_id = :uid THEN b_likes_a ELSE a_likes_b END AS they_gave,
    total_interactions,
    imbalance_ratio,
    relationship_health,
    -- Social debt from :uid's perspective
    -- Positive = :uid owes them, Negative = they owe :uid
    CASE
        WHEN user_a_id = :uid THEN  social_debt_of_a
        ELSE                        -social_debt_of_a
    END AS my_social_debt
FROM v_social_reciprocity
WHERE user_a_id = :uid
   OR user_b_id = :uid
ORDER BY imbalance_ratio DESC
LIMIT 30;


-- Q-N3b: "You owe them" list — users :uid has been receiving
-- engagement from but not reciprocating. The basis for a
-- "Social debt" notification feature.
SELECT
    CASE WHEN user_a_id = :uid THEN user_b ELSE user_a END AS creditor,
    CASE WHEN user_a_id = :uid
         THEN  social_debt_of_a
         ELSE -social_debt_of_a
    END AS debt_amount,
    total_interactions,
    relationship_health
FROM v_social_reciprocity
WHERE (user_a_id = :uid OR user_b_id = :uid)
  AND (
    -- :uid owes them (debt > 0 from :uid perspective)
    (user_a_id = :uid AND social_debt_of_a >  5) OR
    (user_b_id = :uid AND social_debt_of_a < -5)
  )
ORDER BY ABS(social_debt_of_a) DESC
LIMIT 20;


-- Q-N3c: "They owe you" list — users who have heavily engaged
-- with :uid but received nothing back. Powers a "reconnect"
-- or "thank your fans" prompt.
SELECT
    CASE WHEN user_a_id = :uid THEN user_b ELSE user_a END AS fan,
    CASE WHEN user_a_id = :uid
         THEN -social_debt_of_a
         ELSE  social_debt_of_a
    END AS fan_debt_to_me,
    total_interactions,
    relationship_health
FROM v_social_reciprocity
WHERE (user_a_id = :uid OR user_b_id = :uid)
  AND (
    (user_a_id = :uid AND social_debt_of_a < -5) OR
    (user_b_id = :uid AND social_debt_of_a >  5)
  )
ORDER BY ABS(social_debt_of_a) DESC
LIMIT 20;


-- Q-N3d: Platform-wide most imbalanced relationships.
-- Research / moderation query — finds parasocial relationships
-- at scale (e.g. a fan account that obsessively likes a creator
-- who never reciprocates).
SELECT
    user_a,
    user_b,
    a_likes_b,
    b_likes_a,
    imbalance_ratio,
    relationship_health,
    ABS(social_debt_of_a) AS absolute_debt
FROM v_social_reciprocity
WHERE relationship_health IN ('One-sided', 'Parasocial')
  AND total_interactions >= 10
ORDER BY imbalance_ratio DESC, absolute_debt DESC
LIMIT 50;
