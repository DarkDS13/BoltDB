CREATE TRIGGER IF NOT EXISTS trg_comments_parent_same_post
BEFORE INSERT ON comments
FOR EACH ROW
WHEN NEW.parent_comment_id IS NOT NULL
BEGIN
    SELECT
        CASE
            WHEN (
                SELECT post_id
                FROM comments
                WHERE comment_id = NEW.parent_comment_id
            ) <> NEW.post_id THEN RAISE(ABORT, 'Parent comment must belong to same post')
        END;
END;

CREATE TRIGGER IF NOT EXISTS trg_likes_insert_update_post
AFTER INSERT ON likes
FOR EACH ROW
BEGIN
    UPDATE posts
    SET like_count = like_count + 1
    WHERE post_id = NEW.post_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_likes_delete_update_post
AFTER DELETE ON likes
FOR EACH ROW
BEGIN
    UPDATE posts
    SET like_count = CASE WHEN like_count > 0 THEN like_count - 1 ELSE 0 END
    WHERE post_id = OLD.post_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_comments_insert_update_post
AFTER INSERT ON comments
FOR EACH ROW
BEGIN
    UPDATE posts
    SET comment_count = comment_count + 1
    WHERE post_id = NEW.post_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_comments_delete_update_post
AFTER DELETE ON comments
FOR EACH ROW
BEGIN
    UPDATE posts
    SET comment_count = CASE WHEN comment_count > 0 THEN comment_count - 1 ELSE 0 END
    WHERE post_id = OLD.post_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_posts_activity_insert
AFTER INSERT ON posts
FOR EACH ROW
BEGIN
    INSERT INTO user_activity (
        user_id,
        activity_type,
        entity_type,
        entity_id,
        device_type,
        activity_time
    )
    VALUES (
        NEW.user_id,
        'post',
        'post',
        NEW.post_id,
        NEW.source_device,
        NEW.created_at
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_likes_activity_insert
AFTER INSERT ON likes
FOR EACH ROW
BEGIN
    INSERT INTO user_activity (
        user_id,
        activity_type,
        entity_type,
        entity_id,
        device_type,
        activity_time
    )
    VALUES (
        NEW.user_id,
        'like',
        'like',
        NEW.like_id,
        NEW.source_device,
        NEW.created_at
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_comments_activity_insert
AFTER INSERT ON comments
FOR EACH ROW
BEGIN
    INSERT INTO user_activity (
        user_id,
        activity_type,
        entity_type,
        entity_id,
        device_type,
        activity_time
    )
    VALUES (
        NEW.user_id,
        'comment',
        'comment',
        NEW.comment_id,
        NEW.source_device,
        NEW.created_at
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_follows_activity_insert
AFTER INSERT ON follows
FOR EACH ROW
BEGIN
    INSERT INTO user_activity (
        user_id,
        activity_type,
        entity_type,
        entity_id,
        device_type,
        activity_time
    )
    VALUES (
        NEW.follower_user_id,
        'follow',
        'follow',
        NEW.followee_user_id,
        NEW.source_device,
        NEW.followed_at
    );
END;
