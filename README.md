# top-movies

`top-movies` is a tool to match movies from the IMDB database with its corresponding Wikipedia article.

# Requirements

## **Go**

Install Go to build this tool from source depending on your platform. This tool has been built using Go 1.15.

### Testing

The different commands have accompanying unit tests which can be run by `cd cmd/; go test -run ''`

### Dependencies when building from source

- [lib/pq](https://github.com/lib/pq), install by running `go get -u github.com/lib/pq`
- [cobra](https://github.com/spf13/cobra), install by running `go get -u github.com/spf13/cobra`
- [require](https://github.com/stretchr/testify), used for testing, install by running `go get -u github.com/stretchr/testify`

Run `go build` to build the binary `top-movies`

## **Postgres**

The matched data is loaded to a Postgres database. See [the official website](https://www.postgresql.org/download/) to download and install. 

The tool connects to Postgres via a [Connection URI](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING).

It is recommended to use this tool with a standalone database as the tool drops and creates table with each run.

# Data sources

The IMDB dataset version 7 can be downloaded from [here](https://www.kaggle.com/rounakbanik/the-movies-dataset/version/7)

The tool has been designed to work with version 7, in particular it expects columns with certain predefined names.

The Wikipedia dataset can be downloaded from [here](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz)

# Commands

## **ratio**
The `ratio` command calculates the ratio between two columns and outputs results to a new CSV file.

## **match**
The `match` command movies in the IMDB dataset with its corresponding Wikipedia page and outputs results to a new CSV file. 

This uses Go's builtin concurrency model of goroutines and channels to asynchronously read entries from the Wikipedia dataset whilst matching them with movies from the IMDB dataset.

Currently the tool uses movie metadata information and movie credits information to match a movie with its Wikipedia page. Additional features can be added by implementing the `matching` interface and adding the new feature to `features` variable in `match.go`.

## **combine**
The `combine` command combines the movies metadata information with ratio calculations, Wikipedia links/abstract and outputs results to a new CSV file.

## **load**
The `load` command takes the combined dataset and loads it to a Postgres database. This loads the data under the table name `topmovies` containing the following information along with its column name and datatype:
- Title of the film under `title TEXT`
- Budget of the film under `budget BIG INT`
- The date the film was released `year DATE`
- Revenue of the film under `revenue BIG INT`
- Average customer ratings under `ratings REAL`
- Ratio of revenue to budget under `ratio REAL`
- Production companies involved under `production_companies TEXT[]`
- Link to its Wikipedia page under `url TEXT`
- Abstract of the film given by the Wikipedia dataset under `abstract TEXT`

The command drops any existing tables with the name `topmovies` and creates a new one. This allows for any schema changes when the tool is updated. Data can be queried from this table using SQL commands.

## Miscellaneous

A lot of the data from the IMDB dataset have incomplete/malformed input so the tool differentiates between parsing errors, which are collected and output by each command when run with the `-v` flag and do not cause the tool to exit early, versus fatal errors, such as failing to open a file, which causes the tool to exit immediately and output the error.

`run.sh` is a helper script that runs all four commands given the location of the zipped IMDB dataset, location of the gzipped Wikipedia dataset and a Postgres connection URI (in that particular order). The script was checked against [ShellCheck](https://www.shellcheck.net/)


# Next steps
- Add appropriate flags for all commands to pass different columns names. This would be useful when using the tool with a different version of the dataset.

- Currently the data needs to be queried directly from a Postgres table using SQL. Implementing an API would allow non-technical people to query and use the data. The API should allow users to sort films by various categories such as budget, revenue and release year.