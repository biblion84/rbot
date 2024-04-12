package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func main() {

	dbPath := path.Join("data", "submission.db")

	//os.Remove(dbPath)
	//os.Remove(dbPath + "-shm")
	//os.Remove(dbPath + "-wal")

	writeDb, err := OpenDatabase(dbPath)
	p(err)
	readDb, err := OpenDatabase(dbPath)
	p(err)

	p(CreateTables(writeDb))

	if len(os.Args) <= 1 {
		panic("provide a file to open")
	}

	fileToOpen := os.Args[1]

	submissionFile, err := os.Open(path.Join("data", fileToOpen))
	p(err)

	dec := json.NewDecoder(submissionFile)

	inserted := 0

	start := time.Now()

	subreddits, err := GetSubreddits(readDb)
	p(err)

	var tx *sql.Tx

	for dec.More() {
		if inserted%1_000 == 0 {
			if tx != nil {
				p(tx.Commit())
			}
			tx, err = writeDb.Begin()
			p(err)
			fmt.Printf("%d, took %d ms\n", inserted, time.Since(start).Milliseconds())
			start = time.Now()
		}

		var s Submission
		p(dec.Decode(&s))

		if _, found := subreddits[s.Subreddit]; !found {
			err := CreateSubreddit(tx, Subreddit{
				Name:        s.Subreddit,
				Id:          s.SubredditId,
				Subscribers: s.SubredditSubscribers,
				Type:        s.SubredditType,
			})
			subreddits[s.Subreddit] = struct{}{}
			p(err)
		}

		err = CreateSubmission(tx, s)
		if err != nil {
			p(err)
		}
		inserted++
	}

	if tx != nil {
		p(tx.Commit())
	}

	fmt.Println("done")
}

func CreateSubreddit(tx *sql.Tx, sub Subreddit) error {
	_, err := tx.Exec(`INSERT INTO subreddit (id, name, subscribers, type) VALUES (?,?,?,?)`,
		sub.Id, sub.Name, sub.Subscribers, sub.Type)
	return err
}

func CreateSubmission(tx *sql.Tx, s Submission) error {
	_, err := tx.Exec(`INSERT INTO submissions (id, author, author_created_utc, created_utc, domain, is_original_content, is_self, 
              name, num_comments, num_crossposts, over18, pinned, score, subreddit, 
			  thumbnail, title, total_awards_received, upvote_ratio, url, url_overridden_by_dest, view_count) 
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, s.Id, s.Author, s.AuthorCreatedUtc, s.CreatedUtc, s.Domain, s.IsOriginalContent, s.IsSelf,
		s.Name, s.NumComments, s.NumCrossposts, s.Over18, s.Pinned, s.Score, s.Subreddit,
		s.Thumbnail, s.Title, s.TotalAwardsReceived, s.UpvoteRatio, s.Url, s.UrlOverriddenByDest, s.ViewCount,
	)

	return err
}

func GetSubreddits(db *sql.DB) (map[string]struct{}, error) {
	q, err := db.Query(`SELECT name FROM subreddit`)
	if err != nil {
		return nil, err
	}

	names := []string{}

	for q.Next() {

		var name string
		if err := q.Scan(&name); err != nil {
			return nil, err
		}

		names = append(names, name)
	}

	result := make(map[string]struct{}, len(names))

	for _, n := range names {
		result[n] = struct{}{}
	}
	return result, nil
}

func subredditExist(db *sql.DB, subredditName string) bool {
	q, err := db.Query(`SELECT 1 FROM subreddit where name = ?`, subredditName)
	p(err)

	return q.Next()
}

func p(err error) {
	if err != nil {
		panic(err)
	}
}
