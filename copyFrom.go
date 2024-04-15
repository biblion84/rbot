package main

import (
	"encoding/csv"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/goccy/go-json"
	"github.com/klauspost/compress/zstd"
)

func Decode[T any](r io.Reader, out chan T) {

	dec := json.NewDecoder(r)
	e := 0

	for dec.More() {
		var result T
		err := dec.Decode(&result)
		out <- result
		if err != nil {
			e++
			if e > 10000 {
				panic(e)
			}
		}

	}
}

var _REPLACER *strings.Replacer

func init() {
	_REPLACER = strings.NewReplacer("\x00", "", "\n", " ", "\r", " ", "\t", " ", "\r\n", " ", "\"", "", "\\", " ")
}

func sanitize(s string) string {
	return _REPLACER.Replace(s)
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

func Ingest[T any](in chan T, toCsv func(T) []string, dispatch func([][]string)) {
	accLen := 10_000
	accumulator := make([][]string, 0, accLen)
	inFlight := make(chan struct{}, 1)
	for submission := range in {
		accumulator = append(accumulator, toCsv(submission))
		if len(accumulator) == accLen {
			inFlight <- struct{}{}
			dispatch(accumulator)
			accumulator = make([][]string, 0, accLen)
			<-inFlight
		}
	}
}
func DispatchComments(writer *csv.Writer) func(in [][]string) {
	return func(in [][]string) {
		p(writer.WriteAll(in))
	}
}

func toCsvComment(c Comment) []string {
	return []string{sanitize(c.Id), sanitize(c.Text), canonalize(sanitize(c.SubmissionID)), canonalize(sanitize(string(c.ParentID))),
		sanitize(c.Subreddit), sanitize(c.Author), strconv.Itoa(c.Score), "0"}

}

func main() {
	paths, err := filepath.Glob(path.Join("D:", "reddit", "*"))
	p(err)

	for _, filepath := range paths {
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

		csvWriter := csv.NewWriter(os.Stdout)
		csvWriter.Comma = '\t'

		// RS_* for submsissions files, RC_* for comments
		SUBMISSION := strings.Contains(filepath, "RS_")

		if SUBMISSION {
			panic("not implemented")
			submissionChan := make(chan Submission, 100_000)
			Decode(reader, submissionChan)
		} else {
			commentChan := make(chan Comment, 10_000)
			go Ingest(commentChan, toCsvComment, DispatchComments(csvWriter))
			Decode(reader, commentChan)
		}
		csvWriter.Flush()
		file.Close()
		reader.Close()
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()

}

type Submission struct {
	Id                   string          `json:"id"` // primary key
	Author               string          `json:"author"`
	AuthorCreatedUtc     json.RawMessage `json:"author_created_utc"`
	CreatedUtc           json.RawMessage `json:"created_utc"`
	Domain               string          `json:"domain"`
	IsOriginalContent    bool            `json:"is_original_content"`
	IsSelf               bool            `json:"is_self"`
	Name                 string          `json:"name"`
	NumComments          int             `json:"num_comments"`
	NumCrossposts        int             `json:"num_crossposts"`
	Over18               bool            `json:"over_18"`
	Pinned               bool            `json:"pinned"`
	Score                int             `json:"score"`
	Subreddit            string          `json:"subreddit"`
	SubredditId          string          `json:"subreddit_id"`
	SubredditSubscribers int             `json:"subreddit_subscribers"`
	SubredditType        string          `json:"subreddit_type"`
	Thumbnail            string          `json:"thumbnail"`
	Title                string          `json:"title"`
	TotalAwardsReceived  int             `json:"total_awards_received"`
	UpvoteRatio          float64         `json:"upvote_ratio"`
	Url                  string          `json:"url"`
	UrlOverriddenByDest  string          `json:"url_overridden_by_dest"`
	ViewCount            int             `json:"view_count"`
}

type Comment struct {
	Id           string          `json:"id"`
	Text         string          `json:"body"`
	SubmissionID string          `json:"link_id"`
	ParentID     json.RawMessage `json:"parent_id"`
	Subreddit    string          `json:"subreddit"`
	Author       string          `json:"author"`
	Score        int             `json:"score"`
	//CreatedUTC   json.RawMessage `json:"created_utc"`
}

type Subreddit struct {
	Name        string // primary key
	Id          string
	Subscribers int
	Type        string
}

func p(err error) {
	if err != nil {
		panic(err)
	}
}
