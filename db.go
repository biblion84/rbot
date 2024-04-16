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

func OpenSqliteDatabase(path string) (*sql.DB, error) {
	var err error
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA busy_timeout = 5000;
		PRAGMA synchronous = OFF;
		PRAGMA foreign_keys = true;
		PRAGMA cache_size = 7000000;
		PRAGMA temp_store = memory;`)

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

func CreateSqliteTables(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS subreddit (
	name TEXT PRIMARY KEY,
	id TEXT,
	subscribers INTEGER,
	type TEXT
) STRICT;

CREATE TABLE if not exists submission (
    id TEXT PRIMARY KEY,
    author TEXT NOT NULL,
    author_created_utc INTEGER NOT NULL,
    created_utc INTEGER NOT NULL,
    domain TEXT NOT NULL,
    is_original_content INTEGER NOT NULL CHECK (is_original_content IN (0, 1)),
    is_self INTEGER NOT NULL CHECK (is_self IN (0, 1)),
    name TEXT NOT NULL,
    num_comments INTEGER NOT NULL,
    num_crossposts INTEGER NOT NULL,
    over18 INTEGER NOT NULL CHECK (over18 IN (0, 1)),
    pinned INTEGER NOT NULL CHECK (pinned IN (0, 1)),
    score INTEGER NOT NULL,
    subreddit TEXT NOT NULL,
    thumbnail TEXT,
    title TEXT NOT NULL,
    total_awards_received INTEGER NOT NULL,
    upvote_ratio REAL NOT NULL,
    url TEXT,
    url_overridden_by_dest TEXT,
    view_count INTEGER NOT NULL,
    FOREIGN KEY (subreddit) REFERENCES subreddit(name)
) STRICT;

CREATE TABLE if not exists comment (
    id TEXT PRIMARY KEY,
    text TEXT,
    submission_id TEXT,
    parent_id TEXT,
    subreddit TEXT,
    author TEXT,
    score INTEGER,
    created_utc INTEGER,

    FOREIGN KEY (subreddit) REFERENCES subreddit(name),
    FOREIGN KEY (submission_id) REFERENCES submission(id)
) STRICT;


CREATE TABLE if not exists comment_orphan (
    id TEXT PRIMARY KEY,
    text TEXT,
    submission_id TEXT,
    parent_id TEXT,
    subreddit TEXT,
    author TEXT,
    score INTEGER,
    created_utc INTEGER
) STRICT;
-- 
-- 	CREATE INDEX IF NOT EXISTS comment_orphan_parent_idx ON comment_orphan (subreddit);
-- 	CREATE INDEX IF NOT EXISTS subreddit_idx ON submission (subreddit);
-- 	CREATE INDEX IF NOT EXISTS comment_author_idx on comment(author);
-- 	CREATE INDEX IF NOT EXISTS comment_subreddit_idx on comment(subreddit);

CREATE VIRTUAL TABLE IF NOT EXISTS comment_fts USING fts5(
	 id UNINDEXED,
	 text
);

-- Trigger to update the FTS table on insert
CREATE TRIGGER IF NOT EXISTS comment_ai AFTER INSERT ON comment
BEGIN
    INSERT INTO comment_fts (id, text) VALUES (new.id, new.text);
END;

-- Trigger to update the FTS table on update
CREATE TRIGGER IF NOT EXISTS  comment_au AFTER UPDATE ON comment
BEGIN
    UPDATE comment_fts SET text = new.text WHERE id = old.id;
END;

-- Trigger to update the FTS table on delete
CREATE TRIGGER IF NOT EXISTS  comment_ad AFTER DELETE ON comment
BEGIN
    DELETE FROM comment_fts WHERE id = old.id;
END;

`)
	return err
}
