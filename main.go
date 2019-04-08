package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/kevinburke/semaphore"
)

type archiveItem struct {
	Date time.Time
	Name string
}

func (a archiveItem) String() string {
	return a.Name + "\t" + a.Date.Format("2006-01-02 15:04:05")
}

func getArchiveItems(r io.Reader) ([]*archiveItem, error) {
	bs := bufio.NewScanner(r)
	items := make([]*archiveItem, 0)
	for bs.Scan() {
		line := bs.Text()
		if count := strings.Count(line, "\t"); count != 1 {
			return nil, fmt.Errorf("wrong number of tabs in line: want 1 got %d: %q", count, line)
		}
		parts := strings.SplitN(line, "\t", 2)
		// 2018-04-21 08:55:35
		d, err := time.Parse("2006-01-02 15:04:05", parts[1])
		if err != nil {
			return nil, err
		}
		items = append(items, &archiveItem{
			Date: d,
			Name: parts[0],
		})
	}
	if err := bs.Err(); err != nil {
		return nil, err
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Date.Before(items[j].Date)
	})
	return items, nil
}

func dryRunPrint(dryRun bool, args ...interface{}) {
	if dryRun {
		fmt.Println(args...)
	}
}

func main() {
	dryRun := flag.Bool("dry-run", true, "Dry run mode")
	file := flag.String("file", "", "Name of file to load archives from")
	var regex string
	flag.StringVar(&regex, "archive-regex", "", "Regular expression to match archives against")
	flag.Parse()
	if regex == "" {
		log.Fatal("please provide archive regex")
	}
	if regex[0] != '^' {
		regex = ".*" + regex
	}
	if regex[len(regex)-1] != '$' {
		regex = regex + ".*"
	}
	rx, err := regexp.Compile(regex)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var archives io.Reader
	if *file != "" {
		f, err := os.Open(*file)
		if err != nil {
			log.Fatal(err)
		}
		archives = f
	} else {
		buf := new(bytes.Buffer)
		archiveCmd := exec.CommandContext(ctx, "tarsnap", "--list-archives", "-v")
		archiveCmd.Stdout = buf
		if err := archiveCmd.Run(); err != nil {
			log.Fatal(err)
		}
		archives = buf
	}
	items, err := getArchiveItems(archives)
	if err != nil {
		log.Fatal(err)
	}
	matchedItems := make([]*archiveItem, 0)
	for i := range items {
		if !rx.MatchString(items[i].Name) {
			continue
		}
		matchedItems = append(matchedItems, items[i])
	}
	discardItems := make([]*archiveItem, 0)
	currentIndex := 0
	now := time.Now()
	twoYearsAgo := time.Date(now.Year()-2, now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	twoMonthsAgo := time.Date(now.Year(), now.Month()-2, now.Day(), 0, 0, 0, 0, time.UTC)
	for currentIndex < len(matchedItems) {
		dryRunPrint(*dryRun, "keep1", matchedItems[currentIndex].String())
		periodStart := matchedItems[currentIndex].Date
		currentIndex++
		// two years or more ago, one archive per month
		// between two years and two months, one per week
		// sooner than two months, all
		var periodEnd time.Time
		if periodStart.Add(30 * 24 * time.Hour).Before(twoYearsAgo) {
			periodEnd = periodStart.Add(30 * 24 * time.Hour)
		} else if periodStart.Add(7 * 24 * time.Hour).Before(twoMonthsAgo) {
			periodEnd = periodStart.Add(7 * 24 * time.Hour)
		} else {
			currentIndex++
			continue
		}
		for currentIndex < len(matchedItems) {
			if matchedItems[currentIndex].Date.Before(periodEnd) {
				dryRunPrint(*dryRun, "discard", matchedItems[currentIndex].String())
				discardItems = append(discardItems, matchedItems[currentIndex])
				currentIndex++
				continue
			}
			// keep the next item, which is outside the period.
			break
		}
	}
	if *dryRun {
		return
	}
	// Tarsnap does not permit concurrent operations
	s := semaphore.New(1)
	for i := range discardItems {
		s.Acquire()
		go func(name string) {
			buf := new(bytes.Buffer)
			defer s.Release()
			cmd := exec.CommandContext(ctx, "tarsnap", "-d", "-f", name)
			cmd.Stdout = buf
			cmd.Stderr = buf
			if err := cmd.Run(); err != nil {
				if strings.Contains(buf.String(), "Archive does not exist") {
					fmt.Println("gone   ", name)
					return
				}
				cancel()
				io.Copy(os.Stderr, buf)
				log.Fatal(err)
			}
			fmt.Println("deleted", name)
		}(discardItems[i].Name)
	}
}
