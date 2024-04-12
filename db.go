package main

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

func OpenDatabase(path string) (*sql.DB, error) {
	var err error
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA busy_timeout = 5000;
		PRAGMA synchronous = NORMAL;
		PRAGMA cache_size = -1000000000;
		PRAGMA foreign_keys = true;
		PRAGMA temp_store = memory;`)

	return db, err
}

func CreateTables(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS subreddit (
	name TEXT PRIMARY KEY,
	id TEXT,
	subscribers INTEGER,
	type TEXT
);
CREATE TABLE if not exists submissions (
    id TEXT PRIMARY KEY,
    author TEXT,
    author_created_utc INTEGER,
    created_utc INTEGER,
    domain TEXT,
    is_original_content BOOLEAN,
    is_self BOOLEAN,
    name TEXT,
    num_comments INTEGER,
    num_crossposts INTEGER,
    over18 BOOLEAN,
    pinned BOOLEAN,
    score INTEGER,
    subreddit TEXT,
    thumbnail TEXT,
    title TEXT,
    total_awards_received INTEGER,
    upvote_ratio REAL,
    url TEXT,
    url_overridden_by_dest TEXT,
    view_count INTEGER,
    FOREIGN KEY (subreddit) REFERENCES subreddit(name)
);
`)
	return err
}
