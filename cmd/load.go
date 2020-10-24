package cmd

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
)

var (
	loadCmd = &cobra.Command{
		Use:   "load <combined.csv> <connection_url>",
		Short: "Loads the data to a Postgres database",
		RunE:  load,
		Args:  cobra.MinimumNArgs(2),
	}

	dropTableStmt = `
DROP TABLE IF EXISTS topmovies;
`

	createTableStmt = `
CREATE TABLE IF NOT EXISTS topmovies (
	id INTEGER PRIMARY KEY,
	title TEXT,
	year DATE,
	rating REAL,
	budget BIGINT,
	revenue BIGINT,
	ratio REAL,
	production_companies TEXT[],
	url TEXT,
	abstract TEXT
);`

	columns = []string{"id", "title", "year", "rating", "budget", "revenue", "ratio", "production_companies", "url", "abstract"}
)

func load(cmd *cobra.Command, args []string) error {
	// Open combined data file
	data, err := os.Open(args[0])
	if err != nil {
		return err
	}
	defer data.Close()

	stats := makeStats(args[0])
	res := make(map[string]*combinedData)
	err = readCSV(
		csv.NewReader(bufio.NewReader(data)),
		stats,
		columns,
		readCombinedData(res),
	)
	if err != nil {
		return err
	}

	fmt.Print(stats)

	// Create a database connection
	db, err := connect(args[1])
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// Delete existing table
	if _, err := tx.Exec(dropTableStmt); err != nil {
		return err
	}

	// Create table (including any schema changes)
	if _, err := tx.Exec(createTableStmt); err != nil {
		return err
	}

	stmt, err := tx.Prepare(pq.CopyIn("topmovies", columns...))
	if err != nil {
		return err
	}

	// Add data to table
	for _, datum := range res {
		year, err := time.Parse("2006", datum.year)
		if err != nil {
			if verboseErrors {
				fmt.Printf("could not parse year: %v\n", err)
			}
			continue
		}
		_, err = stmt.Exec(datum.id, datum.title, year, datum.rating, datum.budget, datum.revenue, datum.ratio, pq.Array(datum.productionCompanies), datum.url, datum.abstract)
		if err != nil {
			if verboseErrors {
				fmt.Printf("error adding row to table: %v\n", err)
			}
			continue
		}
	}

	// Flush buffer
	if _, err := stmt.Exec(); err != nil {
		return fmt.Errorf("could not flush data: %v", err)
	}

	if err := stmt.Close(); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func connect(url string) (*sql.DB, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	fmt.Println("Successfully connected!")
	return db, nil
}
