package main

type Submission struct {
	Id                   string  `json:"id"` // primary key
	Author               string  `json:"author"`
	AuthorCreatedUtc     int     `json:"author_created_utc"`
	CreatedUtc           int     `json:"created_utc"`
	Domain               string  `json:"domain"`
	IsOriginalContent    bool    `json:"is_original_content"`
	IsSelf               bool    `json:"is_self"`
	Name                 string  `json:"name"`
	NumComments          int     `json:"num_comments"`
	NumCrossposts        int     `json:"num_crossposts"`
	Over18               bool    `json:"over_18"`
	Pinned               bool    `json:"pinned"`
	Score                int     `json:"score"`
	Subreddit            string  `json:"subreddit"`
	SubredditId          string  `json:"subreddit_id"`
	SubredditSubscribers int     `json:"subreddit_subscribers"`
	SubredditType        string  `json:"subreddit_type"`
	Thumbnail            string  `json:"thumbnail"`
	Title                string  `json:"title"`
	TotalAwardsReceived  int     `json:"total_awards_received"`
	UpvoteRatio          float64 `json:"upvote_ratio"`
	Url                  string  `json:"url"`
	UrlOverriddenByDest  string  `json:"url_overridden_by_dest"`
	ViewCount            int     `json:"view_count"`
}

type Subreddit struct {
	Name        string // primary key
	Id          string
	Subscribers int
	Type        string
}
