package main

import (
	"database/sql"
	_ "embed"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func OpenPostgresDatabase() (*sql.DB, error) {
	// Local database, very secure, wow
	db, err := sql.Open("postgres", "user=postgres password=root dbname=postgres sslmode=disable")
	return db, err
}

func CreatePostgresTables(db *sql.DB) error {
	_, err := db.Exec(`
-- DROP TABLE submission;
-- DROP TABLE subreddit;
-- DROP TABLE IF EXISTS comment_orphan;
-- DROP TABLE IF EXISTS submission2;
DROP TABLE IF EXISTS comment2;
-- DROP TABLE IF EXISTS comment;
CREATE TABLE IF NOT EXISTS subreddit (
	name TEXT PRIMARY KEY,
	id TEXT,
	subscribers INTEGER,
	type TEXT
);

CREATE TABLE if not exists submission2 (
    id TEXT PRIMARY KEY,
    author TEXT NOT NULL,
    author_created_utc INTEGER NOT NULL,
    created_utc INTEGER NOT NULL,
    domain TEXT NOT NULL,
    is_original_content BOOLEAN,
    is_self BOOLEAN,
    name TEXT NOT NULL,
    num_comments INTEGER NOT NULL,
    num_crossposts INTEGER NOT NULL,
    over18 BOOLEAN,
    pinned BOOLEAN,
    score INTEGER NOT NULL,
    subreddit TEXT NOT NULL,
    thumbnail TEXT,
    title TEXT NOT NULL,
    total_awards_received INTEGER NOT NULL,
    upvote_ratio REAL NOT NULL,
    url TEXT,
    url_overridden_by_dest TEXT,
    view_count INTEGER NOT NULL
--     FOREIGN KEY (subreddit) REFERENCES subreddit(name)
);

-- the two foreign key, submission_id and subreddit are not marked as such for a reason
-- I'm inserting data month by month, the comments made on January 2023 might have a submission from December 2022 etc, etc..
-- So this is an "orphan" table, if needed I'll create a table with the correct foreign keys once a critical mass of submission have been inserted
CREATE TABLE if not exists comment (
    id TEXT PRIMARY KEY,
    text TEXT,
    submission_id TEXT, -- foreign key on submission(id)
    parent_id TEXT,
    subreddit TEXT, -- foreign key to subreddit(name)
    author TEXT,
    score INTEGER,
    created_utc INTEGER
);
-- CREATE INDEX IF NOT EXISTS comment_submission ON comment (submission_id);
-- CREATE INDEX IF NOT EXISTS comment_subreddit ON comment (subreddit);
-- CREATE INDEX IF NOT EXISTS comment_author_idx on comment(author);
-- CREATE INDEX IF NOT EXISTS submission_author_idx ON submission(author);

-- test db, so that I have a feeling on the index impacts, before creating them on the main table, which takes ages
CREATE TABLE if not exists comment2 (
    id TEXT PRIMARY KEY ,
    text TEXT,
    submission_id TEXT, -- foreign key on submission(id)
    parent_id TEXT,
    subreddit TEXT, -- foreign key to subreddit(name)
    author TEXT,
    score INTEGER,
    created_utc INTEGER
);
-- CREATE INDEX IF NOT EXISTS comment2_subreddit_idx on comment2(subreddit);
`)
	return err
}
