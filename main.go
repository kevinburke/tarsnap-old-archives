package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kevinburke/semaphore"
)

// Tarsnap does not permit concurrent operations
const concurrency = 1

type archiveItem struct {
	Date time.Time
	Name string
}

func (a archiveItem) String() string {
	return a.Name + "\t" + a.Date.Format("2006-01-02 15:04:05")
}

var errAlreadyDeleted = errors.New("archive already deleted")

func deleteArchives(ctx context.Context, archives []string) error {
	args := make([]string, len(archives)*2+1)
	args[0] = "-d"
	for i := range archives {
		args[i*2+1] = "-f"
		args[i*2+2] = archives[i]
	}
	buf := new(bytes.Buffer)
	errBuf := new(bytes.Buffer)
	cmd := exec.CommandContext(ctx, "tarsnap", args...)
	cmd.Stdout = buf
	cmd.Stderr = errBuf
	err := cmd.Run()
	if err != nil {
		if strings.Contains(errBuf.String(), "Archive does not exist") {
			return errAlreadyDeleted
		}
		io.Copy(os.Stderr, errBuf)
		return err
	}
	io.Copy(os.Stderr, errBuf)
	for i := 2; i < len(args); i += 2 {
		fmt.Println("deleted", args[i])
	}
	return nil
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
	batchSize := flag.Int("batch-size", 100, "Batch size")
	// one entry per line
	alreadyDeleted := flag.String("already-deleted-file", "", "Name of file to load already deleted archives from")
	var regex string
	flag.StringVar(&regex, "archive-regex", "", "Regular expression to match archives against")
	flag.Parse()
	if *batchSize <= 0 {
		log.Fatal("please provide a positive batch size")
	}
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
	alreadyDeletedMap := make(map[string]bool)
	if *alreadyDeleted != "" {
		data, err := os.ReadFile(*alreadyDeleted)
		if err != nil {
			log.Fatal(err)
		}
		lines := strings.Split(string(data), "\n")
		for i := 0; i < len(lines); i++ {
			if lines[i] != "" {
				alreadyDeletedMap[lines[i]] = true
			}
		}
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
		tmp, err := os.CreateTemp("", "tarsnap-old-archives-")
		if err == nil {
			io.Copy(tmp, buf)
			fmt.Println("wrote archive output to", tmp.Name())
			tmp.Close()
		}
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
		if alreadyDeletedMap[matchedItems[currentIndex].Name] {
			fmt.Println("gone   ", matchedItems[currentIndex].Name)
			currentIndex++
			continue
		}
		dryRunPrint(*dryRun, "keep", matchedItems[currentIndex].String())
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
			if alreadyDeletedMap[matchedItems[currentIndex].Name] {
				fmt.Println("gone   ", matchedItems[currentIndex].Name)
				currentIndex++
				continue
			}
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
	var wg sync.WaitGroup
	s := semaphore.New(concurrency)
	for i := 0; i < len(discardItems); {
		archives := make([]string, 0)
		initialIndex := i
		for j := initialIndex; j < initialIndex+*batchSize && j < len(discardItems); j++ {
			name := discardItems[j].Name
			if alreadyDeletedMap[name] {
				fmt.Println("gone   ", name)
				continue
			}
			archives = append(archives, name)
			i++
		}
		s.Acquire()
		wg.Add(1)
		go func(archives_ []string) {
			defer s.Release()
			defer wg.Done()
			if err := deleteArchives(ctx, archives); err != nil {
				if err == errAlreadyDeleted {
					// delete one by one
					for i := range archives {
						indivErr := deleteArchives(ctx, []string{archives[i]})
						if indivErr != nil && indivErr != errAlreadyDeleted {
							log.Fatal(indivErr)
						}
						if indivErr == errAlreadyDeleted {
							fmt.Println("gone   ", archives[i])
							continue
						}
					}
				} else if err != nil {
					cancel()
					log.Fatal(err)
				}
			}
		}(archives)
	}
	wg.Wait()
}
