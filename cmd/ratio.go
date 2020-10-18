package cmd

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var (
	ratioCmd = &cobra.Command{
		Use:     "ratio <dataset.csv> [output path]",
		Example: "ratio ~/Downloads/movies.csv",
		Short:   "Calculate the ratio of budget to revenue for movies in the IMDB dataset",
		RunE:    ratio,
		Args:    cobra.MinimumNArgs(1),
	}

	revenueColumn string
	budgetColumn  string
	noHeader      bool
	verboseErrors bool
)

func init() {
	ratioCmd.Flags().StringVarP(&revenueColumn, "revenue-column", "r", "revenue", "the column name/index in input CSV file to use for revenue")
	ratioCmd.Flags().StringVarP(&budgetColumn, "budget-column", "b", "budget", "the column name/index in the input CSV file to use for budget")
	ratioCmd.Flags().BoolVar(&noHeader, "no-header", false, "data starts from the first row, without any headers")
	ratioCmd.Flags().BoolVarP(&verboseErrors, "verbose", "v", false, "output verbose errors")
}

type outputStats struct {
	outputPath          string
	totalRows           int
	parsedRows          int
	budgetRevenueErrors []error
	parseErrors         []error
}

func (o *outputStats) String() string {
	sBuilder := new(strings.Builder)
	fmt.Fprintf(sBuilder, "A total of %d out of %d rows analysed and saved in %s. %d parse errors and %d revenue/budget errors.\n",
		o.parsedRows,
		o.totalRows,
		o.outputPath,
		len(o.parseErrors),
		len(o.budgetRevenueErrors),
	)

	if verboseErrors {
		sBuilder.WriteString("Parse errors:\n")
		for i, err := range o.parseErrors {
			fmt.Fprintf(sBuilder, "error %d: %v\n", i+1, err)
		}

		sBuilder.WriteString("Budget/Revenue errors:\n")
		for i, err := range o.budgetRevenueErrors {
			fmt.Fprintf(sBuilder, "error %d: %v\n", i+1, err)
		}
	} else {
		sBuilder.WriteString("Run tool with -v flag to get verbose error outputs.\n")
	}

	return sBuilder.String()
}

func ratio(cmd *cobra.Command, args []string) error {
	movies := args[0]
	output := "output.csv"
	if len(args) > 1 {
		output = args[1]
	}

	stats := &outputStats{
		outputPath:          output,
		budgetRevenueErrors: make([]error, 0),
		parseErrors:         make([]error, 0),
	}

	file, err := os.Open(movies)
	if err != nil {
		return fmt.Errorf("could not open file at %q: %v", movies, err)
	}
	defer file.Close()
	fin := csv.NewReader(bufio.NewReader(file))

	fileOut, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer fileOut.Close()
	fout := csv.NewWriter(fileOut)

	// TODO: Deal with duplicate column names
	revenueIdx, budgetIdx, err := processHeader(fin, fout, stats)
	if err != nil {
		return fmt.Errorf("could not get column indices for revenue/budget: %v", err)
	}

	if err := processRows(fin, revenueIdx, budgetIdx, fout, stats); err != nil {
		return fmt.Errorf("could not process rows: %v", err)
	}

	fmt.Print(stats)

	return nil
}

func processHeader(fin *csv.Reader, fout *csv.Writer, stats *outputStats) (int, int, error) {
	var revenueIdx, budgetIdx int
	var err error
	if noHeader {
		revenueIdx, err = strconv.Atoi(revenueColumn)
		if err != nil {
			return 0, 0, fmt.Errorf("could not get revenue column index from %s: %v", revenueColumn, err)
		}

		budgetIdx, err = strconv.Atoi(budgetColumn)
		if err != nil {
			return 0, 0, fmt.Errorf("could not get budget column index from %s: %v", budgetColumn, err)
		}
	} else {
		line, done, err := readLine(fin, stats)
		if err != nil {
			return 0, 0, err
		}
		if done {
			return 0, 0, fmt.Errorf("empty file")
		}

		for i, col := range line {
			switch col {
			case revenueColumn:
				revenueIdx = i
			case budgetColumn:
				budgetIdx = i
			}
		}

		line = append(line, "ratio")
		err = fout.Write(line)
		if err != nil {
			return 0, 0, fmt.Errorf("error writing header column to output: %v", err)
		}
	}

	if revenueIdx < 0 || budgetIdx < 0 {
		return 0, 0, fmt.Errorf("revenue/budget column index must not be negative")
	}

	return revenueIdx, budgetIdx, nil
}

func readLine(fin *csv.Reader, stats *outputStats) ([]string, bool, error) {
	record, err := fin.Read()
	if err != nil {
		if err == io.EOF {
			return nil, true, nil
		}

		if parseError, ok := err.(*csv.ParseError); ok {
			stats.parseErrors = append(stats.parseErrors, fmt.Errorf("could not parse line at %d and column %d: %v", parseError.Line, parseError.Column, parseError.Err))
			return []string{}, false, nil
		} else {
			return nil, false, fmt.Errorf("could not read record: %v", err)
		}
	}

	return record, false, err
}

func processRows(fin *csv.Reader, revenueIdx, budgetIdx int, fout *csv.Writer, stats *outputStats) error {
	for {
		stats.totalRows += 1
		record, done, err := readLine(fin, stats)
		if err != nil {
			return fmt.Errorf("could not get next line: %w", err)
		}
		if done {
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
	if err := fout.Error(); err != nil {
		return fmt.Errorf("error flushing contents of output file: %v", err)
	}

	return nil
}
