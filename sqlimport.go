package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
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
			inFLight <- struct{}{}
			go func(accumulator []T) {
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

func canonalize(s string) string {
	return strings.Split(s, "_")[1]
}

// Some comments do not have their foreign keys satisfied
func DispatchComments(in []Comment) {
	tx, err := DB.Begin()
	p(err)

	stmt, err := tx.Prepare(`INSERT INTO comment (id, text, submission_id, parent_id, subreddit, author, score, created_utc)
								VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
	p(err)

	for _, c := range in {
		_, err = stmt.Exec(sanitize(c.ID), sanitize(c.Text), canonalize(sanitize(c.SubmissionID)), canonalize(sanitize(c.ParentID)),
			sanitize(c.Subreddit), sanitize(c.Author), c.Score, c.CreatedUTC)
		if err != nil {
			fmt.Println(c)
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
	db, err := OpenSqliteDatabase("data/submission-test.db")
	p(err)

	p(CreateSqliteTables(db))

	DB = db

	if len(os.Args) <= 1 {
		panic("provide a file to open")
	}

	fileToOpen := os.Args[1]

	file, err := os.Open(path.Join("I:", "Data", fileToOpen))
	p(err)

	// RS_* for submsissions files, RC_* for comments
	SUBMISSION := fileToOpen[:2] == "RS"

	if SUBMISSION {
		submissionChan := make(chan Submission, 100)

		go Ingest(submissionChan, DispatchSubmissions())

		Decode(file, submissionChan)
	} else {
		commentChan := make(chan Comment, 10)

		go Ingest(commentChan, DispatchComments)

		Decode(file, commentChan)
	}

}

func p(err error) {
	if err != nil {
		panic(err)
	}
}
