# ER Model (Social Media DBMS)

```mermaid
erDiagram
    users ||--o{ posts : creates
    users ||--o{ likes : gives
    users ||--o{ comments : writes
    users ||--o{ user_activity : generates
    users ||--o{ follows : follower
    users ||--o{ follows : followee

    posts ||--o{ likes : receives
    posts ||--o{ comments : has
    posts ||--o{ post_tags : tagged_with

    tags ||--o{ post_tags : maps
    comments ||--o{ comments : replies_to

    users {
      int user_id PK
      text username UNIQUE
      text email UNIQUE
      text joined_at
      int is_verified
    }

    posts {
      int post_id PK
      int user_id FK
      text topic
      text created_at
      int like_count
      int comment_count
    }

    likes {
      int like_id PK
      int user_id FK
      int post_id FK
      text created_at
    }

    comments {
      int comment_id PK
      int post_id FK
      int user_id FK
      int parent_comment_id FK
      text created_at
    }

    follows {
      int follower_user_id PK
      int followee_user_id PK
      text followed_at
    }

    tags {
      int tag_id PK
      text tag_name UNIQUE
    }

    post_tags {
      int post_id PK
      int tag_id PK
    }

    user_activity {
      int activity_id PK
      int user_id FK
      text activity_type
      text activity_time
    }
```
