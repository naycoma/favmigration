package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/samber/lo"
)

func main() {
	paths := os.Args[1:]
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if len(paths) > 0 {
		tweets := lo.Must(ReadTweets(paths...))
		lo.Must0(DownloadStatues(ctx, tweets))
	}
	lo.Must0(ExportPublicFromStatuses())
}

func ReadTweets(paths ...string) ([]TweetID, error) {
	favologPaths := []string{}
	tweetsPaths := []string{}
	for _, path := range paths {
		if strings.HasSuffix(path, ".csv") {
			favologPaths = append(favologPaths, path)
		} else {
			tweetsPaths = append(tweetsPaths, path)
		}
	}
	tweetsAll := []TweetID{}
	for _, favologPath := range favologPaths {
		tweets, err := ReadTweetsFromFavologCSV(favologPath)
		if err != nil {
			return tweetsAll, err
		}
		fmt.Printf("[%s] len=%d\n", favologPath, len(tweets))
		tweetsAll = append(tweetsAll, tweets...)
	}
	for _, tweetsPath := range tweetsPaths {
		tweets, err := ReadTweetsFromTxt(tweetsPath)
		if err != nil {
			return tweetsAll, err
		}
		fmt.Printf("[%s] len=%d\n", tweetsPath, len(tweets))
		tweetsAll = append(tweetsAll, tweets...)
	}
	return tweetsAll, nil
}

func ExportPublicFromStatuses() error {
	tweets, err := ReadTweetsFromStatuses()
	if err != nil {
		return err
	}
	return ExportPublic(tweets)
}
