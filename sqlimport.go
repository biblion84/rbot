package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/lib/pq"

	_ "github.com/lib/pq"
)

// global database, because fuck it
var DB *sql.DB

func Decode[T any](r io.Reader, out chan T) {
	dec := json.NewDecoder(r)

	for dec.More() {
		var result T
		p(dec.Decode(&result))
		out <- result
	}
}

func Ingest[T any](in chan T, dispatch func([]T)) {
	accLen := 10_000
	inFLight := make(chan struct{}, runtime.NumCPU())
	accumulator := make([]T, 0, accLen)
	for submission := range in {
		accumulator = append(accumulator, submission)
		if len(accumulator) == accLen {
			go func(accumulator []T) {
				inFLight <- struct{}{}
				start := time.Now()
				dispatch(accumulator)
				fmt.Printf("took %d ms\n", time.Since(start).Milliseconds())
				<-inFLight
			}(accumulator)
			accumulator = make([]T, 0, accLen)
		}
	}
}

func sanitize(s string) string {
	return strings.ReplaceAll(s, "\x00", "")
}

// Some comments do not have their foreign keys satisfied
func DispatchComments(in []Comment) {
	tx, err := DB.Begin()
	p(err)

	stmt, err := tx.Prepare(`INSERT INTO comment (id, text, submission_id, parent_id, subreddit, author, score, created_utc)
								VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
	p(err)

	for _, c := range in {
		_, err := tx.Exec("SAVEPOINT before_comment_insert")
		p(err)
		_, err = stmt.Exec(sanitize(c.ID), sanitize(c.Text), sanitize(c.SubmissionID), sanitize(c.ParentID),
			sanitize(c.Subreddit), sanitize(c.Author), c.Score, c.CreatedUTC)
		if err != nil {
			var pqError *pq.Error
			if errors.As(err, &pqError) {
				// If we have a foreign key issue, as can happen because we're handling data import month/month
				// we'll add the comment in an "orphan" table, which will hopefully be reconciled when the submissions are added
				if pqError.Code == ERR_FOREIGN_KEY_VIOLATION {
					_, err = tx.Exec("ROLLBACK TO SAVEPOINT before_comment_insert")
					p(err)
					_, err = tx.Exec(`INSERT INTO  comment_orphan (id, text, submission_id, parent_id, subreddit,author, score, created_utc)
								VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
						sanitize(c.ID), sanitize(c.Text), sanitize(c.SubmissionID), sanitize(c.ParentID),
						sanitize(c.Subreddit), sanitize(c.Author), c.Score, c.CreatedUTC)
				}
			}
			p(err)
		}
	}
	p(tx.Commit())
}

func DispatchSubmissions() func(in []Submission) {
	subreddits := make(map[string]bool, 1_000_000)
	var subredditsLock sync.Mutex
	return func(in []Submission) {
		subredditsLock.Lock()
		tx, err := DB.Begin()
		p(err)
		for _, s := range in {
			if !subreddits[s.Subreddit] {
				subreddits[s.Subreddit] = true
				_, err := tx.Exec(`INSERT INTO subreddit (id, name, subscribers, type) VALUES ($1, $2, $3, $4)`,
					s.SubredditId, s.Subreddit, s.SubredditSubscribers, s.SubredditType)
				p(err)
			}
		}
		p(tx.Commit())
		subredditsLock.Unlock()

		tx, err = DB.Begin()
		p(err)

		stmt, err := tx.Prepare(`INSERT INTO submission (id, author, author_created_utc, created_utc, domain, is_original_content, is_self, 
              name, num_comments, num_crossposts, over18, pinned, score, subreddit, 
			  thumbnail, title, total_awards_received, upvote_ratio, url, url_overridden_by_dest, view_count) 
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)`)
		p(err)

		for _, s := range in {
			_, err := stmt.Exec(sanitize(s.Id), sanitize(s.Author), s.AuthorCreatedUtc, s.CreatedUtc, sanitize(s.Domain),
				s.IsOriginalContent, s.IsSelf, sanitize(s.Name), s.NumComments, s.NumCrossposts, s.Over18, s.Pinned, s.Score,
				sanitize(s.Subreddit), sanitize(s.Thumbnail), sanitize(s.Title), s.TotalAwardsReceived, s.UpvoteRatio,
				sanitize(s.Url), sanitize(s.UrlOverriddenByDest), s.ViewCount,
			)
			if err != nil {
				fmt.Println(s)
				p(err)
			}
		}

		p(tx.Commit())
	}
}

func main() {
	db, err := OpenPostgresDatabase()
	p(err)

	p(CreatePostgresTables(db))

	DB = db

	if len(os.Args) <= 1 {
		panic("provide a file to open")
	}

	fileToOpen := os.Args[1]

	file, err := os.Open(path.Join("data", fileToOpen))
	p(err)

	// RS_* for submsissions files, RC_* for comments
	SUBMISSION := fileToOpen[:2] == "RS"

	if SUBMISSION {
		submissionChan := make(chan Submission, 10)

		go Ingest(submissionChan, DispatchSubmissions())

		Decode(file, submissionChan)
	} else {
		commentChan := make(chan Comment, 10)

		go Ingest(commentChan, DispatchComments)

		Decode(file, commentChan)
	}

}

//
//func main() {
//
//	dbPath := path.Join("data", "submission3.db")
//	os.Remove(dbPath)
//	os.Remove(dbPath + "-shm")
//	os.Remove(dbPath + "-wal")
//
//	db, err := OpenPostgresDatabase()
//	p(err)
//
//	p(CreatePostgresTables(db))
//
//	if len(os.Args) <= 1 {
//		panic("provide a file to open")
//	}
//
//	fileToOpen := os.Args[1]
//
//	COMMENT := false
//	if fileToOpen[:2] == "RC" {
//		COMMENT = true
//	}
//
//	submissionFile, err := os.Open(path.Join("data", fileToOpen))
//	p(err)
//
//	dec := json.NewDecoder(submissionFile)
//
//	inserted := 0
//
//	start := time.Now()
//
//	subreddits, err := GetSubreddits(db)
//	p(err)
//
//	var tx *sql.Tx
//	var submissionStmt SubmissionStmt
//
//	timer.Begin()
//	skipped := 0
//
//	for dec.More() {
//		if inserted%10_000 == 0 {
//			timer.Start("Transaction")
//			if tx != nil {
//				p(tx.Commit())
//			}
//			tx, err = db.Begin()
//			p(err)
//			p(submissionStmt.PrepatePostgresSubmissionStmt(tx))
//			//p(submissionStmt.PrepareCommentStmt(tx))
//			timer.Stop("Transaction")
//
//			fmt.Printf("%d, took %d ms\n", inserted, time.Since(start).Milliseconds())
//			start = time.Now()
//		}
//		if inserted == 1_000_000 {
//			break
//		}
//		if COMMENT {
//			timer.Start("decode")
//			var c Comment
//			p(dec.Decode(&c))
//			timer.Stop("decode")
//
//			if strings.Contains(c.SubmissionID, "_") {
//				c.SubmissionID = strings.Split(c.SubmissionID, "_")[1]
//			}
//
//			timer.Start("subreddit")
//			if _, found := subreddits[c.Subreddit]; !found {
//				p(CreateOrphanComment(tx, c))
//				timer.Stop("subreddit")
//				continue
//			}
//			timer.Stop("subreddit")
//
//			timer.Start("createComment")
//			err = submissionStmt.CreateComment(c)
//			if err != nil {
//				err = CreateOrphanComment(tx, c)
//			}
//			p(err)
//			timer.Stop("createComment")
//		} else {
//			timer.Start("decode")
//			var s Submission
//			p(dec.Decode(&s))
//			timer.Stop("decode")
//
//			timer.Start("subreddit")
//			if _, found := subreddits[s.Subreddit]; !found {
//				err := CreatePostgresSubreddit(tx, Subreddit{
//					Name:        s.Subreddit,
//					Id:          s.SubredditId,
//					Subscribers: s.SubredditSubscribers,
//					Type:        s.SubredditType,
//				})
//				subreddits[s.Subreddit] = struct{}{}
//				p(err)
//			}
//			timer.Stop("subreddit")
//
//			timer.Start("createSubmission")
//			err = submissionStmt.CreateSubmission(s)
//			p(err)
//			timer.Stop("createSubmission")
//		}
//
//		inserted++
//	}
//
//	if tx != nil {
//		p(tx.Commit())
//	}
//
//	fmt.Printf("skipped : %d\n", skipped)
//
//	timer.End()
//	timer.Print()
//
//	fmt.Println("done")
//}

func CreateSubreddit(tx *sql.Tx, sub Subreddit) error {
	_, err := tx.Exec(`INSERT INTO subreddit (id, name, subscribers, type) VALUES (?,?,?,?)`,
		sub.Id, sub.Name, sub.Subscribers, sub.Type)
	return err
}
func CreatePostgresSubreddit(tx *sql.Tx, sub Subreddit) error {
	_, err := tx.Exec(`INSERT INTO subreddit (id, name, subscribers, type) VALUES ($1, $2, $3, $4)`,
		sub.Id, sub.Name, sub.Subscribers, sub.Type)
	return err
}

type SubmissionStmt struct {
	submissionStmt *sql.Stmt
	commentStmt    *sql.Stmt
}

func (subStmt *SubmissionStmt) PrepatePostgresSubmissionStmt(tx *sql.Tx) error {
	stmt, err := tx.Prepare(`INSERT INTO submission (id, author, author_created_utc, created_utc, domain, is_original_content, is_self, 
              name, num_comments, num_crossposts, over18, pinned, score, subreddit, 
			  thumbnail, title, total_awards_received, upvote_ratio, url, url_overridden_by_dest, view_count) 
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)`)
	subStmt.submissionStmt = stmt
	return err
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
