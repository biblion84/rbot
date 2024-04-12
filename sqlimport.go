package main

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/goccy/go-json"

	_ "github.com/mattn/go-sqlite3"

	"rbot/timer"
)

func main() {

	dbPath := path.Join("data", "submission.db")
	//
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

	COMMENT := false
	if fileToOpen[:2] == "RC" {
		COMMENT = true
	}

	submissionFile, err := os.Open(path.Join("data", fileToOpen))
	p(err)

	dec := json.NewDecoder(submissionFile)

	inserted := 0

	start := time.Now()

	subreddits, err := GetSubreddits(readDb)
	p(err)

	var tx *sql.Tx
	var submissionStmt SubmissionStmt

	timer.Begin()
	skipped := 0

	for dec.More() {
		if inserted%10_000 == 0 {
			timer.Start("Transaction")
			if tx != nil {
				p(tx.Commit())
			}
			tx, err = writeDb.Begin()
			p(err)
			p(submissionStmt.PrepateSubmissionStmt(tx))
			p(submissionStmt.PrepareCommentStmt(tx))
			timer.Stop("Transaction")

			fmt.Printf("%d, took %d ms\n", inserted, time.Since(start).Milliseconds())
			start = time.Now()
		}
		//if inserted == 1_000_000 {
		//	break
		//}
		if COMMENT {
			timer.Start("decode")
			var c Comment
			p(dec.Decode(&c))
			timer.Stop("decode")

			if strings.Contains(c.SubmissionID, "_") {
				c.SubmissionID = strings.Split(c.SubmissionID, "_")[1]
			}

			timer.Start("subreddit")
			if _, found := subreddits[c.Subreddit]; !found {
				p(CreateOrphanComment(tx, c))
				continue
			}
			timer.Stop("subreddit")

			timer.Start("createComment")
			err = submissionStmt.CreateComment(c)
			if err != nil {
				err = CreateOrphanComment(tx, c)
			}
			p(err)
			timer.Stop("createComment")
		} else {
			timer.Start("decode")
			var s Submission
			p(dec.Decode(&s))
			timer.Stop("decode")

			timer.Start("subreddit")
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
			timer.Stop("subreddit")

			timer.Start("createSubmission")
			err = submissionStmt.CreateSubmission(s)
			p(err)
			timer.Stop("createSubmission")
		}

		inserted++
	}

	if tx != nil {
		p(tx.Commit())
	}

	fmt.Printf("skipped : %d\n", skipped)

	timer.End()
	timer.Print()

	fmt.Println("done")
}

func CreateSubreddit(tx *sql.Tx, sub Subreddit) error {
	_, err := tx.Exec(`INSERT INTO subreddit (id, name, subscribers, type) VALUES (?,?,?,?)`,
		sub.Id, sub.Name, sub.Subscribers, sub.Type)
	return err
}

type SubmissionStmt struct {
	submissionStmt *sql.Stmt
	commentStmt    *sql.Stmt
}

func (subStmt *SubmissionStmt) PrepateSubmissionStmt(tx *sql.Tx) error {
	stmt, err := tx.Prepare(`INSERT INTO submission (id, author, author_created_utc, created_utc, domain, is_original_content, is_self, 
              name, num_comments, num_crossposts, over18, pinned, score, subreddit, 
			  thumbnail, title, total_awards_received, upvote_ratio, url, url_overridden_by_dest, view_count) 
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	subStmt.submissionStmt = stmt
	return err
}

func (subStmt *SubmissionStmt) PrepareCommentStmt(tx *sql.Tx) error {
	stmt, err := tx.Prepare(`INSERT INTO comment (id, text, submission_id, parent_id, subreddit, author, score, created_utc)
								   VALUES (?, ?, ?, ?, ?, ?, ?, ?);`)
	subStmt.commentStmt = stmt
	return err
}
func CreateOrphanComment(tx *sql.Tx, c Comment) error {
	_, err := tx.Exec(`INSERT INTO comment_orphan (id, text, submission_id, parent_id, subreddit, author, score, created_utc)
								   VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
		c.ID, c.Text, c.SubmissionID, c.ParentID, c.Subreddit, c.Author, c.Score, c.CreatedUTC)
	return err
}

func (subStmt *SubmissionStmt) CreateSubmission(s Submission) error {
	_, err := subStmt.submissionStmt.Exec(s.Id, s.Author, s.AuthorCreatedUtc, s.CreatedUtc, s.Domain, s.IsOriginalContent, s.IsSelf,
		s.Name, s.NumComments, s.NumCrossposts, s.Over18, s.Pinned, s.Score, s.Subreddit,
		s.Thumbnail, s.Title, s.TotalAwardsReceived, s.UpvoteRatio, s.Url, s.UrlOverriddenByDest, s.ViewCount,
	)

	return err
}
func (subStmt *SubmissionStmt) CreateComment(c Comment) error {
	_, err := subStmt.commentStmt.Exec(c.ID, c.Text, c.SubmissionID, c.ParentID, c.Subreddit, c.Author, c.Score, c.CreatedUTC)

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
