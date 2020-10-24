package cmd

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	combineCmd = &cobra.Command{
		Use:   "combine <movies_metadata.csv> <ratio.csv> <wiki_matches.csv> <ratings.csv>",
		Short: "Combine the tagged movied with metadata information",
		RunE:  combine,
		Args:  cobra.ExactArgs(4),
	}
)

func combine(cmd *cobra.Command, args []string) error {
	metadataFile, err := os.Open(args[0])
	if err != nil {
		return err
	}
	defer metadataFile.Close()

	moviesMetadataStats := makeStats(args[0])
	moviesMetadata := make(moviesMetadata)
	err = readCSV(
		csv.NewReader(bufio.NewReader(metadataFile)),
		moviesMetadataStats,
		[]string{"id", "title", "budget", "revenue", "release_date", "production_companies", "original_title"},
		readMoviesMetadata(moviesMetadata),
	)
	if err != nil {
		return err
	}

	fmt.Print(moviesMetadataStats)

	ratioFile, err := os.Open(args[1])
	if err != nil {
		return err
	}
	defer ratioFile.Close()

	ratioStats := makeStats(args[1])
	moviesRatios := make(moviesRatios)
	err = readCSV(
		csv.NewReader(bufio.NewReader(ratioFile)),
		ratioStats,
		[]string{"id", "ratio"},
		readMoviesRatio(moviesRatios),
	)
	if err != nil {
		return err
	}

	fmt.Print(ratioStats)

	matchingFile, err := os.Open(args[2])
	if err != nil {
		return err
	}
	defer matchingFile.Close()

	wikiStats := makeStats(args[2])
	wikiMatches := make(wikiMatches)
	err = readCSV(
		csv.NewReader(bufio.NewReader(matchingFile)),
		wikiStats,
		[]string{"id", "abstract", "url", "score"},
		readWikiMatches(wikiMatches),
	)
	if err != nil {
		return err
	}

	fmt.Print(wikiStats)

	ratingFile, err := os.Open(args[3])
	if err != nil {
		return err
	}
	defer ratingFile.Close()

	ratingsStats := makeStats(args[3])
	ratings := make(ratings)
	err = readCSV(
		csv.NewReader(bufio.NewReader(ratingFile)),
		ratingsStats,
		[]string{"movieId", "userId", "rating"},
		readMoviesRating(ratings),
	)
	if err != nil {
		return err
	}

	fmt.Print(ratingsStats)

	fout, err := os.Create("output_combine.csv")
	if err != nil {
		return err
	}
	defer fout.Close()

	writer := csv.NewWriter(fout)
	if err := writer.Write([]string{"id", "title", "url", "abstract", "score", "budget", "year", "revenue", "ratio", "rating", "production_companies"}); err != nil {
		return err
	}

	for id, info := range moviesMetadata {
		if match, ok := wikiMatches[id]; ok {
			writer.Write([]string{id, info.title, match.url, match.abstract, match.score, info.budget, info.year, info.revenue, moviesRatios.forID(id), ratings.forID(id), info.production})
		}
	}

	writer.Flush()
	return writer.Error()
}
