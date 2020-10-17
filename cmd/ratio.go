package cmd

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

var (
	ratioCmd = &cobra.Command{
		Use:     "ratio <dataset.csv> [output path]",
		Example: "ratio ~/Downloads/movies.csv",
		Short:   "Calculate the budget to revenue ratio of movies",
		RunE:    ratio,
		Args:    cobra.MinimumNArgs(1),
	}

	revenueColumn string
	budgetColumn  string
	verboseErrors bool
)

func init() {
	ratioCmd.Flags().StringVarP(&revenueColumn, "revenue-column-name", "r", "revenue", "the column name in input CSV file to use for revenue")
	ratioCmd.Flags().StringVarP(&budgetColumn, "budget-column-name", "b", "budget", "the column name in the input CSV file to use for budget")
	ratioCmd.Flags().BoolVarP(&verboseErrors, "verbose", "v", false, "output verbose errors")
}

type outputStats struct {
	totalRows           int
	parsedRows          int
	budgetRevenueErrors []error
	parseErrors         []error
}

func ratio(cmd *cobra.Command, args []string) error {
	movies := args[0]
	output := "output.csv"
	if len(args) > 1 {
		output = args[1]
	}
	stats := &outputStats{
		budgetRevenueErrors: make([]error, 0),
		parseErrors:         make([]error, 0),
	}

	file, err := os.Open(movies)
	if err != nil {
		return fmt.Errorf("could not open file at %q: %v", movies, err)
	}
	defer file.Close()

	fin := csv.NewReader(bufio.NewReader(file))
	columns, exists, err := readLine(fin, stats)
	if err != nil {
		return fmt.Errorf("could not get columns: %w", err)
	}
	if !exists {
		return fmt.Errorf("file %q is empty", movies)
	}

	// TODO: Deal with duplicate column names
	var revenueIdx, budgetIdx int
	for i, col := range columns {
		switch col {
		case revenueColumn:
			revenueIdx = i
		case budgetColumn:
			budgetIdx = i
		}
	}

	fileOut, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer fileOut.Close()

	fout := csv.NewWriter(fileOut)
	columns = append(columns, "ratio")
	err = fout.Write(columns)
	if err != nil {
		return fmt.Errorf("error writing header column to output: %v", err)
	}

	for {
		stats.totalRows += 1
		record, exists, err := readLine(fin, stats)
		if err != nil {
			return fmt.Errorf("could not get next line: %w", err)
		}
		if !exists {
			// We have reached end of file
			break
		}

		if len(record) <= revenueIdx || len(record) <= budgetIdx {
			stats.budgetRevenueErrors = append(stats.budgetRevenueErrors, fmt.Errorf("insufficient columns at line %d", stats.totalRows))
			continue
		}

		revenue, err := strconv.Atoi(record[revenueIdx])
		if err != nil {
			stats.budgetRevenueErrors = append(stats.budgetRevenueErrors, fmt.Errorf("could not convert revenue %s at line %d", record[revenueIdx], stats.totalRows))
			continue
		}

		budget, err := strconv.Atoi(record[budgetIdx])
		if err != nil {
			stats.budgetRevenueErrors = append(stats.budgetRevenueErrors, fmt.Errorf("could not convert budget %s at line %d", record[budgetIdx], stats.totalRows))
			continue
		}

		if revenue == 0 || budget == 0 {
			stats.budgetRevenueErrors = append(stats.budgetRevenueErrors, fmt.Errorf("revenue/budget zero at line %d", stats.totalRows))
			continue
		}

		ratio := float64(revenue) / float64(budget)
		record = append(record, fmt.Sprintf("%f", ratio))

		if err := fout.Write(record); err != nil {
			return fmt.Errorf("error writing line: %v", err)
		}

		stats.parsedRows += 1
	}

	fout.Flush()
	if err = fout.Error(); err != nil {
		return fmt.Errorf("error flushing contents of output file: %v", err)
	}

	fmt.Printf("A total of %d out of %d rows analysed and saved in %s. %d parse errors and %d revenue/budget errors. Run tool with -v flag to get verbose error outputs\n",
		stats.parsedRows,
		stats.totalRows,
		output,
		len(stats.parseErrors),
		len(stats.budgetRevenueErrors),
	)

	if verboseErrors {
		fmt.Println("Parse errors:")
		for i, err := range stats.parseErrors {
			fmt.Printf("%d: %v\n", i, err)
		}

		fmt.Println("Budget/Revenue errors:")
		for i, err := range stats.budgetRevenueErrors {
			fmt.Printf("%d: %v\n", i, err)
		}
	}

	return nil
}

func readLine(fin *csv.Reader, stats *outputStats) ([]string, bool, error) {
	record, err := fin.Read()
	if err != nil {
		if err == io.EOF {
			return nil, false, nil
		}

		if parseError, ok := err.(*csv.ParseError); ok {
			stats.parseErrors = append(stats.parseErrors, fmt.Errorf("could not parse line at %d and column %d: %v", parseError.Line, parseError.Column, parseError.Err))
			return []string{}, true, nil
		} else {
			return nil, false, fmt.Errorf("could not read record: %v", err)
		}
	}

	return record, true, err
}
