package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type FavologItem struct {
	TweetID    TweetID
	Timestamp  time.Time
	ScreenName string
	Text       string
	Tag        []string
}

func parseFavologRowItem(record []string) (FavologItem, error) {
	item := FavologItem{}
	if len(record) != 5 {
		return item, fmt.Errorf("invalid record length: %d", len(record))
	}

	id, err := strconv.Atoi(record[0])
	if err != nil {
		return item, err
	}
	item.TweetID = TweetID(id)

	date, err := time.Parse("060102 150405", record[1])
	if err != nil {
		return item, err
	}
	item.Timestamp = date

	item.ScreenName = record[2]
	item.Text = record[3]
	item.Tag = strings.Split(record[4], " ")
	return item, nil
}

func readCSV(csvFilePath string) ([][]string, error) {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	return records, nil
}

func readFavologCSV(csvFilePath string) ([]FavologItem, error) {
	records, err := readCSV(csvFilePath)
	if err != nil {
		return nil, err
	}

	items := []FavologItem{}
	for _, record := range records {
		item, err := parseFavologRowItem(record)
		if err != nil {
			return items, err
		}
		items = append(items, item)
	}
	return items, nil
}

func ReadTweetsFromFavologCSV(csvFilePath string) ([]TweetID, error) {
	logItems, err := readFavologCSV(csvFilePath)
	if err != nil {
		return nil, err
	}
	var tweets []TweetID
	for _, row := range logItems {
		tweets = append(tweets, row.TweetID)
	}
	return tweets, nil
}

var muStatus = regexp.MustCompile(`(\d+)$`)

func ReadTweetsFromTxt(path string) ([]TweetID, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	tweets := []TweetID{}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		matches := muStatus.FindStringSubmatch(line)
		if len(matches) != 2 {
			return tweets, fmt.Errorf("invalid status: %s", line)
		}
		status, err := strconv.Atoi(matches[1])
		if err != nil {
			return tweets, fmt.Errorf("invalid status: %s", line)
		}
		tweets = append(tweets, TweetID(status))
	}
	return tweets, nil
}
