#!/usr/bin/env python3
"""Run SQL-heavy analytical workloads against the social media DBMS demo."""

from __future__ import annotations

import argparse
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "social_media.db"


@dataclass(frozen=True)
class QueryDef:
    name: str
    description: str
    sql: str
    params_factory: Callable[[argparse.Namespace, sqlite3.Connection], dict]


def choose_default_user(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT user_id
        FROM users
        ORDER BY RANDOM()
        LIMIT 1
        """
    ).fetchone()
    return int(row[0])


def choose_default_post(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT post_id
        FROM comments
        GROUP BY post_id
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """
    ).fetchone()
    if row:
        return int(row[0])
    row = conn.execute("SELECT post_id FROM posts ORDER BY RANDOM() LIMIT 1").fetchone()
    return int(row[0])


def query_catalog() -> dict[str, QueryDef]:
    return {
        "top_influencers": QueryDef(
            name="top_influencers",
            description="Rank users by followers + engagement using window functions.",
            sql="""
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
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {"limit": args.limit},
        ),
        "trending_topics": QueryDef(
            name="trending_topics",
            description="Find top topics in the last N days by weighted engagement.",
            sql="""
                WITH recent AS (
                    SELECT post_id, topic, like_count, comment_count
                    FROM posts
                    WHERE created_at >= datetime('now', :window)
                )
                SELECT
                    topic,
                    COUNT(*) AS posts_in_window,
                    SUM(like_count) AS likes_in_window,
                    SUM(comment_count) AS comments_in_window,
                    SUM(like_count + 2 * comment_count) AS trend_score,
                    DENSE_RANK() OVER (ORDER BY SUM(like_count + 2 * comment_count) DESC) AS trend_rank
                FROM recent
                GROUP BY topic
                ORDER BY trend_score DESC, posts_in_window DESC
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {"window": f"-{args.days} days", "limit": args.limit},
        ),
        "trending_tags": QueryDef(
            name="trending_tags",
            description="Join posts/tags to compute high-performing tags.",
            sql="""
                SELECT
                    t.tag_name,
                    COUNT(DISTINCT p.post_id) AS tagged_posts,
                    SUM(p.like_count) AS likes,
                    SUM(p.comment_count) AS comments,
                    SUM(p.like_count + 2 * p.comment_count) AS weighted_score,
                    RANK() OVER (ORDER BY SUM(p.like_count + 2 * p.comment_count) DESC) AS trend_rank
                FROM posts p
                JOIN post_tags pt ON pt.post_id = p.post_id
                JOIN tags t ON t.tag_id = pt.tag_id
                WHERE p.created_at >= datetime('now', :window)
                GROUP BY t.tag_name
                ORDER BY weighted_score DESC
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {"window": f"-{args.days} days", "limit": args.limit},
        ),
        "recommended_posts": QueryDef(
            name="recommended_posts",
            description="Collaborative filtering query for a target user.",
            sql="""
                WITH liked_by_target AS (
                    SELECT post_id
                    FROM likes
                    WHERE user_id = :user_id
                ),
                similar_users AS (
                    SELECT l2.user_id, COUNT(*) AS overlap
                    FROM likes l1
                    JOIN likes l2 ON l1.post_id = l2.post_id
                    WHERE l1.user_id = :user_id
                      AND l2.user_id <> :user_id
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
                    ROUND(c.support + p.like_count * 0.10 + p.comment_count * 0.20, 2) AS recommendation_score
                FROM candidates c
                JOIN posts p ON p.post_id = c.post_id
                ORDER BY recommendation_score DESC
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {
                "user_id": args.user_id if args.user_id else choose_default_user(conn),
                "limit": args.limit,
            },
        ),
        "comment_thread": QueryDef(
            name="comment_thread",
            description="Recursive CTE to fetch hierarchical comment chains.",
            sql="""
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
                    WHERE c.post_id = :post_id
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
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {
                "post_id": args.post_id if args.post_id else choose_default_post(conn),
                "limit": args.limit,
            },
        ),
        "activity_spikes": QueryDef(
            name="activity_spikes",
            description="Detect hourly spikes using rolling-window analytics.",
            sql="""
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
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {"limit": args.limit},
        ),
        "feed_unseen_posts": QueryDef(
            name="feed_unseen_posts",
            description="Fetch timeline posts from followed users not liked by target user.",
            sql="""
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
                LEFT JOIN likes l
                    ON l.user_id = f.follower_user_id
                   AND l.post_id = p.post_id
                WHERE f.follower_user_id = :user_id
                  AND l.like_id IS NULL
                ORDER BY p.created_at DESC
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {
                "user_id": args.user_id if args.user_id else choose_default_user(conn),
                "limit": args.limit,
            },
        ),
        "cohort_retention": QueryDef(
            name="cohort_retention",
            description="Calculate month-by-month retention for each signup cohort.",
            sql="""
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
                ORDER BY j.cohort_month, j.month_number
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {"limit": args.limit},
        ),

        # ── NOVELTY 1: Echo Chamber Coefficient ──────────────────────────────
        "echo_chamber_leaderboard": QueryDef(
            name="echo_chamber_leaderboard",
            description="Platform-wide bubble leaderboard — HHI score per user (0=diverse, 1=full bubble).",
            sql="""
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
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {"limit": args.limit},
        ),
        "diversity_nudge_feed": QueryDef(
            name="diversity_nudge_feed",
            description="Posts from topics user :uid almost never engages with — the feed no platform provides.",
            sql="""
                WITH user_top2_topics AS (
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
                    (p.like_count + 2 * p.comment_count) AS engagement_score,
                    COALESCE(vq.cqi, 0)            AS conversation_quality,
                    'diversity_nudge'               AS feed_source
                FROM posts p
                JOIN users u ON u.user_id = p.user_id
                LEFT JOIN v_conversation_quality vq ON vq.post_id = p.post_id
                WHERE p.topic NOT IN (SELECT topic FROM user_top2_topics)
                  AND p.post_id NOT IN (SELECT post_id FROM already_interacted)
                  AND p.created_at >= datetime('now', '-30 days')
                ORDER BY
                    (p.like_count + 2 * p.comment_count) * 0.6 +
                    COALESCE(vq.cqi, 0) * 0.4 DESC
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {
                "uid":   getattr(args, "uid", None) or choose_default_user(conn),
                "limit": args.limit,
            },
        ),
        "user_topic_breakdown": QueryDef(
            name="user_topic_breakdown",
            description="Per-topic engagement share for a user — shows their bubble composition.",
            sql="""
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
                grand AS (SELECT SUM(total) AS g FROM aggregated)
                SELECT
                    a.topic,
                    a.total                                   AS interactions,
                    ROUND(100.0 * a.total / g.g, 1)          AS pct_of_total,
                    ROUND(a.total * 1.0 / g.g, 4)            AS share
                FROM aggregated a, grand g
                ORDER BY a.total DESC;
            """,
            params_factory=lambda args, conn: {
                "uid": getattr(args, "uid", None) or choose_default_user(conn),
            },
        ),

        # ── NOVELTY 2: Conversation Quality Index ─────────────────────────────
        "best_discussions_cqi": QueryDef(
            name="best_discussions_cqi",
            description="Posts ranked by conversation quality (depth × diversity × author-response), not raw likes.",
            sql="""
                SELECT
                    post_id,
                    username,
                    topic,
                    like_count,
                    comment_count,
                    unique_commenters,
                    ROUND(reply_ratio * 100, 1)   AS reply_pct,
                    ROUND(commenter_diversity, 2) AS commenter_diversity,
                    author_replies,
                    cqi,
                    cqi_rank_global
                FROM v_conversation_quality
                WHERE comment_count >= 3
                  AND created_at >= datetime('now', '-14 days')
                ORDER BY cqi_rank_global
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {"limit": args.limit},
        ),

        # ── NOVELTY 3: Social Reciprocity Score ───────────────────────────────
        "relationship_health": QueryDef(
            name="relationship_health",
            description="All interactions for user :uid ranked by imbalance — who you neglect and who neglects you.",
            sql="""
                SELECT
                    CASE WHEN user_a_id = :uid THEN user_b    ELSE user_a    END AS other_user,
                    CASE WHEN user_a_id = :uid THEN a_likes_b ELSE b_likes_a END AS i_gave,
                    CASE WHEN user_a_id = :uid THEN b_likes_a ELSE a_likes_b END AS they_gave,
                    total_interactions,
                    imbalance_ratio,
                    relationship_health,
                    CASE
                        WHEN user_a_id = :uid THEN  social_debt_of_a
                        ELSE                        -social_debt_of_a
                    END AS my_social_debt
                FROM v_social_reciprocity
                WHERE user_a_id = :uid
                   OR user_b_id = :uid
                ORDER BY imbalance_ratio DESC
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {
                "uid":   getattr(args, "uid", None) or choose_default_user(conn),
                "limit": args.limit,
            },
        ),
        "social_debt_owe": QueryDef(
            name="social_debt_owe",
            description="Users whose engagement user :uid has not reciprocated — social debt owed by the user.",
            sql="""
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
                    (user_a_id = :uid AND social_debt_of_a >  2) OR
                    (user_b_id = :uid AND social_debt_of_a < -2)
                  )
                ORDER BY ABS(social_debt_of_a) DESC
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {
                "uid":   getattr(args, "uid", None) or choose_default_user(conn),
                "limit": args.limit,
            },
        ),
        "social_debt_owe_me": QueryDef(
            name="social_debt_owe_me",
            description="Users who engage with :uid but receive nothing back — fans the user ignores.",
            sql="""
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
                    (user_a_id = :uid AND social_debt_of_a < -2) OR
                    (user_b_id = :uid AND social_debt_of_a >  2)
                  )
                ORDER BY ABS(social_debt_of_a) DESC
                LIMIT :limit;
            """,
            params_factory=lambda args, conn: {
                "uid":   getattr(args, "uid", None) or choose_default_user(conn),
                "limit": args.limit,
            },
        ),
    }


def render_table(columns: list[str], rows: list[tuple]) -> str:
    if not rows:
        return "(no rows)"

    text_rows = [["NULL" if v is None else str(v) for v in row] for row in rows]
    widths = [len(col) for col in columns]
    for row in text_rows:
        for idx, value in enumerate(row):
            widths[idx] = min(max(widths[idx], len(value)), 64)

    def clip(value: str, width: int) -> str:
        if len(value) <= width:
            return value
        return value[: width - 3] + "..."

    divider = "-+-".join("-" * w for w in widths)
    header = " | ".join(columns[i].ljust(widths[i]) for i in range(len(columns)))
    lines = [header, divider]

    for row in text_rows:
        lines.append(" | ".join(clip(row[i], widths[i]).ljust(widths[i]) for i in range(len(columns))))
    return "\n".join(lines)


def run_query(
    conn: sqlite3.Connection,
    query_def: QueryDef,
    args: argparse.Namespace,
    explain: bool,
) -> None:
    params = query_def.params_factory(args, conn)
    sql = query_def.sql
    if explain:
        sql = f"EXPLAIN QUERY PLAN {sql.strip()}"

    print(f"\n=== {query_def.name} ===")
    print(f"{query_def.description}")
    if params:
        print(f"params: {params}")

    start = time.perf_counter()
    cursor = conn.execute(sql, params)
    rows = cursor.fetchall()
    elapsed = time.perf_counter() - start

    columns = [desc[0] for desc in cursor.description] if cursor.description else []
    print(render_table(columns, rows))
    print(f"rows: {len(rows)} | time: {elapsed * 1000:.2f} ms")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run analytical SQL queries for the DBMS project.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to SQLite database file.")
    parser.add_argument(
        "--query",
        default="all",
        help="Query name from catalog, or 'all'. Use --list to see available queries.",
    )
    parser.add_argument("--list", action="store_true", help="List available query names and descriptions.")
    parser.add_argument("--limit", type=int, default=10, help="Maximum rows to return for each query.")
    parser.add_argument("--days", type=int, default=14, help="Time window in days for trending queries.")
    parser.add_argument("--user-id", type=int, help="Optional user_id for personalized queries.")
    parser.add_argument("--post-id", type=int, help="Optional post_id for comment thread query.")
    parser.add_argument("--explain", action="store_true", help="Show query plans using EXPLAIN QUERY PLAN.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    catalog = query_catalog()

    if args.list:
        print("Available queries:")
        for item in catalog.values():
            print(f"- {item.name:20s} {item.description}")
        return

    if not args.db.exists():
        raise FileNotFoundError(f"Database file not found: {args.db}")

    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row

    try:
        if args.query == "all":
            for item in catalog.values():
                run_query(conn, item, args, explain=args.explain)
        else:
            if args.query not in catalog:
                raise ValueError(f"Unknown query '{args.query}'. Use --list to inspect valid names.")
            run_query(conn, catalog[args.query], args, explain=args.explain)
    finally:
        conn.close()


if __name__ == "__main__":
    main()