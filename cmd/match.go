package cmd

import (
	"bufio"
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"os"
	"strings"

	"github.com/dghubble/trie"
	"github.com/spf13/cobra"
)

var (
	matchCmd = &cobra.Command{
		Use:   "match <wiki.xml> <movies_metadata.csv> <movies_credits.csv>",
		Short: "Match movies in the IMDB dataset with its corresponding Wikipedia page",
		RunE:  match,
		Args:  cobra.ExactArgs(3),
	}
)

func match(cmd *cobra.Command, args []string) error {
	// Read Wiki file
	wikiPath := args[0]
	wikiFile, err := os.Open(wikiPath)
	if err != nil {
		return err
	}
	defer wikiFile.Close()

	wikiDecoder := xml.NewDecoder(wikiFile)
	movieEntries := make(chan *wikiEntry, 1000)

	// Asynchronously read Wikipedia data
	go func() {
		if err := readWiki(wikiDecoder, movieEntries); err != nil {
			fmt.Printf("error reading wiki dataset: %v", err)
		}
	}()

	// Read movies metadata dataset
	metadataPath := args[1]
	metadataFile, err := os.Open(metadataPath)
	if err != nil {
		return err
	}
	defer metadataFile.Close()

	moviesMetadataStats := makeStats(metadataPath)
	moviesMetadata := make(moviesMetadata)
	err = readCSV(
		csv.NewReader(bufio.NewReader(metadataFile)),
		moviesMetadataStats,
		[]string{"id", "title", "release_date", "production_companies", "original_title"},
		readMoviesMetadata(moviesMetadata),
	)
	if err != nil {
		return err
	}

	fmt.Print(moviesMetadataStats)

	// Read movies credits dataset
	creditsPath := args[2]
	creditsFile, err := os.Open(creditsPath)
	if err != nil {
		return err
	}
	defer creditsFile.Close()

	moviesCreditsStats := makeStats(creditsPath)
	moviesCredits := make(moviesCredits)
	err = readCSV(
		csv.NewReader(bufio.NewReader(creditsFile)),
		moviesCreditsStats,
		[]string{"id", "crew", "cast"},
		readMoviesCredits(moviesCredits),
	)
	if err != nil {
		return err
	}

	fmt.Print(moviesCreditsStats)

	// Intialise features from movies datasets
	features := []matching{
		moviesMetadata.features(),
		moviesCredits.features(),
	}

	results := map[string]*matchResult{}
	for entry := range movieEntries {
		mostRelevantIDs := []string{}

		normalisedEntry := &wikiEntry{
			title:    normaliseString(entry.title),
			abstract: normaliseString(entry.abstract),
		}

		// Load list of relevant movie IDs
		for _, feature := range features {
			mostRelevantIDs = append(mostRelevantIDs, feature.mostRelevant(normalisedEntry)...)
		}

		var maxScore float64
		var bestID string
		for _, id := range mostRelevantIDs {
			var score float64
			for _, feature := range features {
				score += feature.relevance(normalisedEntry, id)
			}

			score = score / float64(len(features))
			if score > maxScore {
				bestID = id
				maxScore = score
			}
		}

		if maxScore > 0 {
			if currentRes, ok := results[bestID]; ok {
				if currentRes.score > maxScore {
					continue
				}
			}
			results[bestID] = &matchResult{
				score:    maxScore,
				url:      entry.url,
				abstract: entry.abstract,
			}

			// Output progress for information
			if len(results)%1000 == 0 {
				fmt.Printf("%d films matched\n", len(results))
			}

		}
	}

	fmt.Printf("A total of %d out of %d movies were matched with a Wikipedia entry\n", len(results), len(moviesMetadata))

	// Write results
	fout, err := os.Create("output_matching.csv")
	if err != nil {
		return err
	}
	defer fout.Close()
	writer := csv.NewWriter(fout)
	if err := writer.Write([]string{"id", "url", "abstract", "score"}); err != nil {
		return err
	}
	for id, res := range results {
		writer.Write([]string{id, res.url, res.abstract, fmt.Sprintf("%f", res.score)})
	}
	writer.Flush()

	return writer.Error()
}

// matching provides the specification for features to match against a wikipedia entry
type matching interface {
	// mostRevelant returns a list of most relevant ids in the channel given a wikipedia entry.
	// WaitGroup.Done() must be called when no further ids are going to be sent.
	mostRelevant(*wikiEntry) []string
	// relevance calculates a score between 0 and 1 given a wiki entry and an id
	relevance(*wikiEntry, string) float64
}

var _ matching = (*moviesMetadataFeatures)(nil)

func (m *moviesMetadataFeatures) mostRelevant(e *wikiEntry) []string {
	title := []rune(e.title)
	return m.trie.walk(title)
}

func walkTrie(out chan<- string) trie.WalkFunc {
	return func(key string, value interface{}) error {
		if ids, ok := value.([]string); ok {
			for _, id := range ids {
				out <- id
			}
		}
		return nil
	}
}

func (m *moviesMetadataFeatures) relevance(e *wikiEntry, id string) float64 {
	md, ok := m.data[id]
	if !ok {
		return 0
	}

	var tokenScore float64
	for _, token := range md.tokens {
		if strings.Contains(e.abstract, token) {
			tokenScore += 1
		}
	}
	tokenScore = tokenScore / float64(len(md.tokens))

	var titleScore float64
	if md.title != "" && strings.Contains(e.title, md.title) {
		titleScore = float64(len(md.title)) / float64(len(e.title))
	}

	if titleScore == 0 && md.originalTitle != "" && strings.Contains(e.title, md.originalTitle) {
		titleScore = float64(len(md.originalTitle)) / float64(len(e.title))
	}

	titleBias := 0.5
	score := ((1 - titleBias) * tokenScore) + (titleBias * titleScore)

	return score
}

var _ matching = moviesCreditsFeatures(nil)

func (m moviesCreditsFeatures) mostRelevant(e *wikiEntry) []string {
	return nil
}

func (m moviesCreditsFeatures) relevance(e *wikiEntry, id string) float64 {
	md, ok := m[id]
	if !ok {
		return 0
	}
	var score, total float64
	for _, token := range md {
		if strings.Contains(e.abstract, token) {
			score += 1
		}
		total += 1
	}

	if total == 0 {
		return 0
	}

	return score / total
}

type matchResult struct {
	score    float64
	url      string
	abstract string
}
