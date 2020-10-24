package cmd

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	ratioCmd = &cobra.Command{
		Use:     "ratio <dataset.csv> [output path]",
		Example: "ratio ~/Downloads/movies.csv",
		Short:   "Calculate the ratio between two columns of a CSV dataset",
		RunE:    ratio,
		Args:    cobra.MinimumNArgs(1),
	}

	revenueColumn string
	budgetColumn  string
)

func init() {
	ratioCmd.Flags().StringVar(&revenueColumn, "columnA", "revenue", "the column name/index in input CSV file to use for the numerator in calculating the ratio")
	ratioCmd.Flags().StringVar(&budgetColumn, "columnB", "budget", "the column name/index in the input CSV file to use for the denominator in calculating the ratio")
}

func ratio(cmd *cobra.Command, args []string) error {
	movies := args[0]
	output := "output_ratio.csv"
	if len(args) > 1 {
		output = args[1]
	}

	file, err := os.Open(movies)
	if err != nil {
		return fmt.Errorf("could not open file at %q: %v", movies, err)
	}
	defer file.Close()
	fin := csv.NewReader(bufio.NewReader(file))

	moviesMetadataStats := makeStats(movies)
	moviesMetadata := make(map[string]*movieMetadata)
	err = readCSV(
		fin,
		moviesMetadataStats,
		[]string{"id", revenueColumn, budgetColumn},
		readMoviesMetadata(moviesMetadata),
	)
	if err != nil {
		return err
	}

	fileOut, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer fileOut.Close()
	fout := csv.NewWriter(fileOut)

	// Write column names of output file
	fout.Write([]string{"id", "ratio"})

	var numSkipped int
	for id, metadata := range moviesMetadata {
		if metadata.revenue <= 0 || metadata.budget <= 0 {
			numSkipped++
			continue
		}

		ratio := float64(metadata.revenue) / float64(metadata.budget)
		fout.Write([]string{id, fmt.Sprintf("%f", ratio)})
	}

	fout.Flush()

	fmt.Printf("%d rows had 0 revenue/budget\n", numSkipped)
	fmt.Print(moviesMetadataStats)

	return fout.Error()
}
