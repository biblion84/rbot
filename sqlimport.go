package main

import (
	"database/sql"
	//"encoding/json"
	"fmt"
	"io"
	//_ "net/http/pprof"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/klauspost/compress/zstd"
)

// global database, because fuck it
var DB *sql.DB

func Decode[T any](r io.Reader, out chan T, reject chan error) {

	dec := json.NewDecoder(r)
	i := 0
	start := time.Now()

	for dec.More() {
		var result T
		err := dec.Decode(&result)
		out <- result
		if err != nil {
			reject <- err
		}
		i++
		if i%10_000 == 0 {
			fmt.Printf("%d %dms elapsed\t", i, time.Since(start).Milliseconds())
			start = time.Now()
		}

	}
}

func Ingest[T any](in chan T, dispatch func([]T)) {
	accLen := 100_000

	i := 0
	inFLight := make(chan struct{}, runtime.NumCPU())
	accumulator := make([]T, 0, accLen)
	for submission := range in {
		accumulator = append(accumulator, submission)
		if len(accumulator) == accLen {
			i++
			//start := time.Now()
			inFLight <- struct{}{}
			go func(accumulator []T) {
				dispatch(accumulator)
				//fmt.Printf("took %d ms\t", time.Since(start).Milliseconds())
				<-inFLight
			}(accumulator)
			fmt.Println(i)
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

// Some comments do not have their foreign keys satisfied
func DispatchComments(in []Comment) {
	tx, err := DB.Begin()
	p(err)

	stmt, err := tx.Prepare(`INSERT INTO comment (id, text, submission_id, parent_id, subreddit, author, score, created_utc)
								VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING`)
	p(err)

	for _, c := range in {
		_, err = stmt.Exec(sanitize(c.Id), sanitize(c.Text), canonalize(sanitize(c.SubmissionID)), canonalize(sanitize(string(c.ParentID))),
			sanitize(c.Subreddit), sanitize(c.Author), c.Score, "0")
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

		stmt, err := tx.Prepare(`INSERT INTO submission2 (id, author, author_created_utc, created_utc, domain, is_original_content, is_self, 
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
	//go func() {
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()
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

			//go func() {
			//	i := 0
			//	for {
			//		<-submissionChan
			//		i++
			//		if i%10_000 == 0 {
			//			fmt.Printf("%d ingested\n", i)
			//		}
			//	}
			//}()

			Decode(reader, submissionChan, rejectChan)
		} else {
			commentChan := make(chan Comment, 10)

			go Ingest(commentChan, DispatchComments)

			//go func() {
			//	for {
			//		<-commentChan
			//	}
			//}()

			Decode(reader, commentChan, rejectChan)
		}
		file.Close()
		reader.Close()

		fmt.Printf("took %d s\t", time.Since(start).Seconds())
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()

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
