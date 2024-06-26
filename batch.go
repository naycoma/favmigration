package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/samber/lo"
)

type TweetID int

func (t *TweetID) String() string {
	return fmt.Sprintf("%d", *t)
}

type BatchID string

func NewBatchID(batchCreatedAt time.Time) BatchID {
	return BatchID(fmt.Sprintf("%d-auto", batchCreatedAt.UnixNano()/int64(ROUND_CREATED_AT)))
}

const (
	DEFAULT_MAX_ITEMS_PER_PAGE  = 10
	DEFAULT_MAX_ITEMS_IN_HEADER = 20
)

type Batch struct {
	BatchID   BatchID
	Tweets    []TweetID
	IsHead    bool
	CreatedAt time.Time
}

func newMergedBatch(createdAt time.Time, merged []TweetID) *Batch {
	return &Batch{
		BatchID:   NewBatchID(createdAt),
		Tweets:    merged,
		IsHead:    false,
		CreatedAt: createdAt,
	}
}
func newHeadBatch(createdAt time.Time, head []TweetID) *Batch {
	return &Batch{
		BatchID:   NewBatchID(createdAt),
		Tweets:    head,
		IsHead:    true,
		CreatedAt: createdAt,
	}
}

func (b *Batch) String() string {
	if b.IsHead {
		return fmt.Sprintf("HeadBatch{BatchID: %s, Len: %v}", b.BatchID, len(b.Tweets))
	}
	return fmt.Sprintf("MergedBatch{BatchID: %s, Len: %v}", b.BatchID, len(b.Tweets))
}

func (b *Batch) SplitPages(createdAt time.Time) []*Page {
	if b.IsHead {
		return lo.Map(b.Tweets, func(tweet TweetID, i int) *Page {
			path := fmt.Sprintf("head/%s/%d", b.BatchID, tweet)
			return &Page{
				ID: path,
				Statuses: []IDAndTimestamp{
					{
						ID:        fmt.Sprintf("%d", tweet),
						Timestamp: uint(createdAt.Unix()),
					},
				},
				CreatedAt: uint(createdAt.Unix()),
			}
		})
	}
	return lo.Map(lo.Chunk(SortNew2Old(b.Tweets), DEFAULT_MAX_ITEMS_PER_PAGE), func(pageTweets []TweetID, i int) *Page {
		return &Page{
			ID: fmt.Sprintf("merged/%s/%06d", b.BatchID, i+1),
			Statuses: lo.Map(pageTweets, func(tweet TweetID, _ int) IDAndTimestamp {
				return IDAndTimestamp{
					ID:        fmt.Sprintf("%d", tweet),
					Timestamp: uint(createdAt.Unix()),
				}
			}),
			CreatedAt: uint(createdAt.Unix()),
		}
	})
}

func (b *Batch) LinkPages(next BatchID) (*BatchLinked, []*Page) {
	var nextPtr *BatchID
	if lo.IsNotEmpty(next) {
		nextPtr = &next
	}
	pages := b.SplitPages(b.CreatedAt)
	return &BatchLinked{
		ID:   b.BatchID,
		Head: b.IsHead,
		Pages: lo.Map(pages, func(page *Page, i int) string {
			return page.ID
		}),
		Next:        nextPtr,
		CreatedAt:   uint(time.Now().Unix()),
		UpdatedAt:   uint(time.Now().Unix()),
		UpdateNonce: generateNonce(),
	}, pages
}

type IDAndTimestamp struct {
	ID        string `json:"id"`
	Timestamp uint   `json:"ts"`
}

type Page struct {
	// page下のpathでもある
	ID        string           `json:"id"`
	Statuses  []IDAndTimestamp `json:"statuses"`
	CreatedAt uint             `json:"created_at"`
}

// バッチの単方向連結リスト
type BatchLinked struct {
	ID          BatchID  `json:"id"`
	Head        bool     `json:"head"`
	Pages       []string `json:"pages"`
	Next        *BatchID `json:"next"`
	CreatedAt   uint     `json:"created_at"`
	UpdatedAt   uint     `json:"updated_at"`
	UpdateNonce string   `json:"update_nonce"`
}

func (b *BatchLinked) String() string {
	if b.Head {
		return fmt.Sprintf("HeadLink-%s%v", b.ID, b.Pages)
	}
	return fmt.Sprintf("MergedLink-%s%v", b.ID, b.Pages)
}

func ExportPublic(tweets []TweetID) error {
	if err := os.RemoveAll("data/public"); err != nil {
		return err
	}
	var prevBatchID BatchID
	var currentJson CurrentJson
	for _, batch := range Tweets2Batches(tweets) {
		if batch.IsHead {
			currentJson.Head = &batch.BatchID
		} else {
			currentJson.Last = &batch.BatchID
		}
		linked, pages := batch.LinkPages(prevBatchID)
		log.Println(linked)
		prevBatchID = batch.BatchID
		if err := CreateJson(fmt.Sprintf("data/public/batches/%s.json", batch.BatchID), linked); err != nil {
			return err
		}
		for _, page := range pages {
			if err := CreateJson(fmt.Sprintf("data/public/pages/%s.json", page.ID), page); err != nil {
				return err
			}
		}
	}
	currentJson.UpdatedAt = uint(time.Now().Unix())
	return CreateJson("data/public/current.json", currentJson)
}

type CurrentJson struct {
	Head      *BatchID `json:"head"`
	Last      *BatchID `json:"last"`
	UpdatedAt uint     `json:"updated_at"`
}

// Old to New
func Tweets2Batches(tweets []TweetID) []*Batch {
	if len(tweets) == 0 {
		return nil
	}
	splitTweets := lo.Chunk(SortOld2New(tweets), DEFAULT_MAX_ITEMS_IN_HEADER)
	splitTweets = lo.Map(splitTweets, func(batchTweets []TweetID, _ int) []TweetID {
		return SortNew2Old(batchTweets)
	})
	mergedTweetsAll, headTweets := splitTweets[:len(splitTweets)-1], splitTweets[len(splitTweets)-1]
	newCreateAt := createdAtFactory(time.Now())
	batches := []*Batch{}
	for _, mergedTweets := range mergedTweetsAll {
		batches = append(batches, newMergedBatch(newCreateAt(), mergedTweets))
	}
	batches = append(batches, newHeadBatch(newCreateAt(), headTweets))
	return batches
}

const ROUND_CREATED_AT = time.Microsecond * 100

func createdAtFactory(createdAt time.Time) (next func() time.Time) {
	current := createdAt.Truncate(ROUND_CREATED_AT)
	return func() time.Time {
		now := time.Now().Truncate(ROUND_CREATED_AT)
		if now.After(current) {
			current = now
		} else {
			current = current.Add(ROUND_CREATED_AT)
		}
		return current
	}
}

func generateNonce() string {
	str, err := generateRandomURLSafeBase64(32)
	if err != nil {
		panic(err)
	}
	return str
}

const URL_SAFE_BASE64_CHARSET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"

func generateRandomURLSafeBase64(length int) (string, error) {
	if length < 0 {
		return "", fmt.Errorf("invalid length: %d", length)
	}
	if length == 0 {
		return "", nil
	}

	const charset = URL_SAFE_BASE64_CHARSET
	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b), nil
}

func SortOld2New(tweets []TweetID) []TweetID {
	slice := tweets[:]
	sort.SliceStable(slice, func(i, j int) bool {
		return slice[i] < slice[j]
	})
	return slice
}
func SortNew2Old(tweets []TweetID) []TweetID {
	slice := tweets[:]
	sort.SliceStable(slice, func(i, j int) bool {
		return slice[i] > slice[j]
	})
	return slice
}
