package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	progressbar "github.com/schollz/progressbar/v3"
	"golang.org/x/time/rate"
)

func DownloadStatues(ctx context.Context, tweets []TweetID) error {
	needDownloads := lo.Reject(tweets, func(tweetID TweetID, _ int) bool {
		statues, ok := existStatues(tweetID)
		return ok && statues.Success()
	})
	barCount := len(needDownloads)
	bar := progressbar.Default(int64(barCount), "downloading")
	successTweets := []TweetID{}
	notfoundTweets := []TweetID{}
	errorTweets := []TweetID{}

	var wg sync.WaitGroup
	var mu sync.Mutex
	maxRetries := 5
	rm := NewRetryManager(rate.Every(time.Second/15), maxRetries)

	for _, tweetID := range needDownloads {
		wg.Add(1)
		go func(tweetID TweetID) {
			defer wg.Done()
			defer bar.Add(1)
			statues, err := rm.Download(ctx, tweetID)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errorTweets = append(errorTweets, tweetID)
				fmt.Println("download error:", err)
				return
			}
			if err := putStatues(statues); err != nil {
				errorTweets = append(errorTweets, tweetID)
				fmt.Println("export error:", err)
				return
			}
			if statues.Success() {
				successTweets = append(successTweets, tweetID)
			} else {
				notfoundTweets = append(notfoundTweets, tweetID)
			}
		}(tweetID)
	}

	wg.Wait()
	bar.Finish()
	fmt.Println(
		"downloaded",
		"successes=", len(successTweets),
		"errors=", len(errorTweets),
		"not_found=", len(notfoundTweets),
	)
	return nil
}

func NewRetryManager(limit rate.Limit, maxRetries int) *RetryManager {
	lm := rate.NewLimiter(limit, 1)
	return &RetryManager{
		limiter:    lm,
		limit:      limit,
		maxRetries: maxRetries,
	}
}

type RetryManager struct {
	limiter        *rate.Limiter
	limit          rate.Limit
	mu             sync.Mutex
	waitMu         sync.Mutex
	maxRetries     int
	rateLimitLevel uint
}

func (rm *RetryManager) Wait(ctx context.Context) error {
	rm.waitMu.Lock()
	defer rm.waitMu.Unlock()
	n := 1 << rm.getRateLimitLevel()
	limit := rm.limit / rate.Limit(n)
	if limit != rm.limiter.Limit() {
		rm.limiter.SetLimit(limit)
	}
	return rm.limiter.Wait(ctx)
}

func (rm *RetryManager) getRateLimitLevel() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if int(rm.rateLimitLevel) > rm.maxRetries {
		return rm.maxRetries
	}
	return int(rm.rateLimitLevel)
}

func (rm *RetryManager) Done() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.rateLimitLevel > 0 {
		rm.rateLimitLevel--
	}
}
func (rm *RetryManager) Fail() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rm.rateLimitLevel < uint(rm.maxRetries) {
		rm.rateLimitLevel++
	}
}

func (rm *RetryManager) Download(ctx context.Context, tweetID TweetID) (*Statues, error) {
	var statues *Statues
	var err error
	for retries := 0; retries < rm.maxRetries; retries++ {
		if err := rm.Wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limiter error: %w", err)
		}
		statues, err = downloadStatues(ctx, tweetID, time.Now())
		if !errors.Is(err, ErrTooManyRequests) {
			rm.Done()
			return statues, nil
		}
		rm.Fail()
	}
	return statues, fmt.Errorf("retry limit exceeded: %w", err)
}

func downloadStatues(ctx context.Context, tweetID TweetID, savedAt time.Time) (*Statues, error) {
	statues := &Statues{
		ID:       tweetID.String(),
		Complete: true,
		SavedAt:  uint(savedAt.Unix()),
	}
	fixTweet, err := fxt(ctx, tweetID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return statues, nil
		}
		return statues, err
	}
	if fixTweet.Success() {
		statues.FxtwitterData = &fixTweet
	}
	return statues, nil
}

func existStatues(tweetID TweetID) (statues Statues, ok bool) {
	path := fmt.Sprintf("data/private/statuses/%s.json", tweetID.String())
	if _, err := os.Stat(path); err != nil {
		return statues, false
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return statues, false
	}
	if err := json.Unmarshal(b, &statues); err != nil {
		return statues, false
	}
	return statues, true
}

func putStatues(statues *Statues) error {
	b, err := json.MarshalIndent(statues, "", "  ")
	if err != nil {
		return err
	}
	return Create(fmt.Sprintf("data/private/statuses/%s.json", statues.ID), b)
}

type Statues struct {
	ID            string          `json:"id"`
	Complete      bool            `json:"complete"`
	FxtwitterData *FixTwitterJson `json:"fxtwitter_data"`
	SavedAt       uint            `json:"saved_at"`
}

func (s Statues) Success() bool {
	if s.FxtwitterData == nil {
		return false
	}
	return s.FxtwitterData.Success()
}

func (s Statues) Timestamp() (ts uint, ok bool) {
	if !s.Success() {
		return
	}
	if tweet, ok := s.FxtwitterData.ParseTweet(); ok {
		return tweet.CreatedTimestamp, true
	}
	return
}

type Author struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	ScreenName string `json:"screen_name"`
}

type Tweet struct {
	ID               string `json:"id"`
	Text             string `json:"text"`
	CreatedTimestamp uint   `json:"created_timestamp"`
	Author           Author `json:"author"`
}

type FixTwitterJson struct {
	Code    uint            `json:"code"`
	Message string          `json:"message"`
	Tweet   json.RawMessage `json:"tweet"`
}

func (f *FixTwitterJson) Success() bool {
	if f == nil {
		return false
	}
	return f.Code == 200 && f.Message == "OK"
}

func (f *FixTwitterJson) ParseTweet() (tweet Tweet, ok bool) {
	if !f.Success() {
		return
	}
	if err := json.Unmarshal(f.Tweet, &tweet); err != nil {
		return
	}
	return tweet, true
}

func fxt(ctx context.Context, tweetID TweetID) (FixTwitterJson, error) {
	var data FixTwitterJson
	body, err := fxtBody(ctx, tweetID)
	if err != nil {
		return data, err
	}
	if err = json.Unmarshal(body, &data); err != nil {
		return data, err
	}
	return data, nil
}

const FIX_TWITTER_API_URL = "https://api.fxtwitter.com"

func fxtBody(ctx context.Context, tweetID TweetID) ([]byte, error) {
	url := FIX_TWITTER_API_URL + "/status/" + tweetID.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	switch response.StatusCode {
	case http.StatusOK:
		return body, nil
	case http.StatusNotFound:
		return body, ErrNotFound
	case http.StatusTooManyRequests:
		return nil, ErrTooManyRequests
	}
	if strings.HasPrefix(string(body), "error code:") {
		return nil, fmt.Errorf("error code: %s", strings.TrimPrefix(string(body), "error code: "))
	}
	return body, fmt.Errorf("status code: %d", response.StatusCode)
}

var (
	ErrNotFound        = errors.New("not found")
	ErrTooManyRequests = errors.New("too many requests")
)

func ReadTweetsFromStatuses() (tweets []TweetID, err error) {
	files, err := filepath.Glob("data/private/statuses/*.json")
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		tweet, err := path2tweetID(file)
		if err != nil {
			return nil, err
		}
		tweets = append(tweets, tweet)
	}
	return tweets, nil
}

func path2tweetID(path string) (TweetID, error) {
	fileName := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	i, err := strconv.Atoi(fileName)
	if err != nil {
		return 0, err
	}
	return TweetID(i), nil
}
