package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	// this is a 2x faster drop-in replacement for encoding/json
	"github.com/goccy/go-json"
	"github.com/klauspost/compress/zstd"
)

// This script is used to export the reddit dumps into a postgres sql table
// on my hardware I get a sustained 150k rows inserted per second
// it stills takes hours to load all the data, if you want to use this for your own project, you'll need to at least update
// some hardcoded paths in this file and the hardcoded credentials in db.go
// you can use either sqlite or postgres
// 	I found that I insert faster in postgres, index creation is also a lot faster
// 	the single thread nature of sqlite as it turns out was limiting the ingestion speed

// global database, because fuck it
var DB *sql.DB

func Decode[T any](r io.Reader, out chan T, reject chan error) {

	dec := json.NewDecoder(r)
	for dec.More() {
		var result T
		err := dec.Decode(&result)
		out <- result
		if err != nil {
			reject <- err
		}
	}
}

func prettyPrint(x int) string {
	printed := []rune(strconv.Itoa(x))

	prettyPrinted := ""

	for i := 0; i < len(printed); i++ {
		if i%3 == 0 && i != 0 {
			prettyPrinted = "_" + prettyPrinted
		}

		prettyPrinted = string(printed[len(printed)-1-i]) + prettyPrinted
	}

	return prettyPrinted
}

func Ingest[T any](in chan T, dispatch func([]T)) {
	accLen := 10_000
	beginning := time.Now()
	i := 0
	inFLight := make(chan struct{}, runtime.NumCPU())
	accumulator := make([]T, 0, accLen)
	for submission := range in {
		accumulator = append(accumulator, submission)
		if len(accumulator) == accLen {
			i++
			inFLight <- struct{}{}
			start := time.Now()
			go func(i int, accumulator []T) {
				dispatch(accumulator)
				<-inFLight
				if (i*accLen)%10_000 == 0 {
					fmt.Printf("%s: total : %sms took %sms\n", prettyPrint(i*accLen),
						prettyPrint(int(time.Since(beginning).Milliseconds())), prettyPrint(int(time.Since(start).Milliseconds())))
				}
			}(i, accumulator)
			accumulator = make([]T, 0, accLen)
		}
	}
}

func sanitize(s string) string {
	return strings.ReplaceAll(s, "\x00", "")
}

func sanitizeInteger(raw json.RawMessage) string {
	if string(raw) == "null" {
		return "0"
	}
	split := strings.Split(string(raw), ".")[0]
	if split == "" {
		return "0"
	}
	return split
}

func canonalize(s string) string {
	split := strings.Split(s, "_")
	if len(split) > 1 {
		return split[1]
	}
	return s
}

func DispatchComments(in []Comment) {
	tx, err := DB.Begin()
	p(err)

	stmt, err := tx.Prepare(`INSERT INTO comment (id, text, submission_id, parent_id, subreddit, author, score, created_utc)
								VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING`)
	p(err)

	for _, c := range in {
		_, err = stmt.Exec(sanitize(c.Id), sanitize(c.Text), canonalize(sanitize(c.SubmissionID)), canonalize(sanitize(string(c.ParentID))),
			sanitize(c.Subreddit), sanitize(c.Author), c.Score, sanitizeInteger(c.CreatedUTC))
		if err != nil {
			fmt.Println(c)
			p(err)
		}
	}
	p(tx.Commit())
}

func DispatchSubmissions(subreddits map[string]bool) func(in []Submission) {
	var subredditsLock sync.Mutex
	return func(in []Submission) {
		missingSubreddits := []Subreddit{}

		subredditsLock.Lock()
		for _, s := range in {
			if !subreddits[s.Subreddit] {
				missingSubreddits = append(missingSubreddits, Subreddit{
					Name:        s.Subreddit,
					Id:          s.SubredditId,
					Subscribers: s.SubredditSubscribers,
					Type:        s.SubredditType,
				})
				subreddits[s.Subreddit] = true
			}
		}
		subredditsLock.Unlock()

		if len(missingSubreddits) > 0 {
			tx, err := DB.Begin()
			p(err)
			stmt, err := tx.Prepare(`INSERT INTO subreddit (id, name, subscribers, type) VALUES ($1, $2, $3, $4)`)
			p(err)
			for _, s := range missingSubreddits {
				_, err := stmt.Exec(s.Id, s.Name, s.Subscribers, s.Type)
				p(err)
			}
			p(tx.Commit())
		}

		tx, err := DB.Begin()
		p(err)

		stmt, err := tx.Prepare(`INSERT INTO submission (id, author, author_created_utc, created_utc, domain, is_original_content, is_self, 
              name, num_comments, num_crossposts, over18, pinned, score, subreddit, 
			  thumbnail, title, total_awards_received, upvote_ratio, url, url_overridden_by_dest, view_count) 
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
              ON CONFLICT (id) DO NOTHING`)
		p(err)

		for _, s := range in {
			_, err := stmt.Exec(sanitize(s.Id), sanitize(s.Author), sanitizeInteger(s.AuthorCreatedUtc), sanitizeInteger(s.CreatedUtc), sanitize(s.Domain),
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

	paths, err := filepath.Glob(path.Join("D:", "reddit", "*"))

	subreddits, err := GetSubreddits(db)
	p(err)

	dispatchSubmissions := DispatchSubmissions(subreddits)

	rejectChan := make(chan error, 1)
	go func() {
		errorLogged := 0
		errorFile, err := os.OpenFile("err.txt", os.O_TRUNC|os.O_CREATE, 0640)
		p(err)
		for {
			err := <-rejectChan
			errorLogged++
			if errorLogged > 10_000 {
				panic("too many error logged")
			}
			errorFile.Write([]byte(err.Error()))
		}
	}()

	for _, filepath := range paths {
		fmt.Printf("starting on %s\n", filepath)
		file, err := os.Open(filepath)
		p(err)

		var reader io.ReadCloser

		if strings.HasSuffix(filepath, ".zst") {
			zstdReader, err := zstd.NewReader(file, zstd.WithDecoderMaxWindow(1<<31), zstd.WithDecoderLowmem(false))
			p(err)
			reader = zstdReader.IOReadCloser()
		} else {
			reader = file
		}

		// RS_* for submsissions files, RC_* for comments
		SUBMISSION := strings.Contains(filepath, "RS_")

		start := time.Now()

		if SUBMISSION {
			submissionChan := make(chan Submission, 100_000)
			go Ingest(submissionChan, dispatchSubmissions)
			Decode(reader, submissionChan, rejectChan)
		} else {
			commentChan := make(chan Comment, 10)
			go Ingest(commentChan, DispatchComments)
			Decode(reader, commentChan, rejectChan)
		}
		file.Close()
		reader.Close()

		fmt.Printf("took %d s\t", time.Since(start).Seconds())
	}

	db.Close()
}

func GetSubreddits(db *sql.DB) (map[string]bool, error) {
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

	result := make(map[string]bool, len(names))

	for _, n := range names {
		result[n] = true
	}
	return result, nil
}

func p(err error) {
	if err != nil {
		panic(err)
	}
}
