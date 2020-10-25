# top-movies

`top-movies` is a tool to match movies from the IMDB database with its corresponding Wikipedia article.

# Requirements

## **Go**

[Install Go](https://golang.org/dl/) to build this tool from source for your platform. This tool has been built using Go 1.15.

### Dependencies when building from source

- [lib/pq](https://github.com/lib/pq), install by running `go get -u github.com/lib/pq`
- [cobra](https://github.com/spf13/cobra), install by running `go get -u github.com/spf13/cobra`
- [require](https://github.com/stretchr/testify), used for testing, install by running `go get -u github.com/stretchr/testify`

Run `go build` to build the binary `top-movies`

### Testing

The different commands have accompanying unit tests which can be run by `cd cmd/; go test -run ''`

## **Postgres**

The matched data is loaded to a Postgres database. See [the official website](https://www.postgresql.org/download/) on how to download and install. 

The tool connects to Postgres by specifying a [Connection URI](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING).

It is recommended to use this tool with a standalone database as the tool drops and creates tables with each run.

# Data sources

The IMDB dataset version 7 can be downloaded from [here](https://www.kaggle.com/rounakbanik/the-movies-dataset/version/7). The tool has been designed to work with version 7, in particular it expects columns with certain predefined names.

The Wikipedia dataset can be downloaded from [here](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz)

# Commands

## **ratio**
The `ratio` command calculates the ratio between two columns and outputs results to a new CSV file.

## **match**
The `match` command links the movies in the IMDB dataset with its corresponding Wikipedia page (if it finds one) and outputs the results to a new CSV file. 

This command uses Go's builtin concurrency model of goroutines and channels to asynchronously read entries from the Wikipedia dataset whilst matching them with movies from the IMDB dataset.

Movies are matched to their Wikipedia article by populating a trie with movies titles from the IMDB dataset and doing a prefix search using the title of a Wikipedia article as the key. If multiple matches are found then a score is calculated based on the movie title, Wikipedia title, presence of various keywords in the abstract such as release date, cast members and production crew. The movie with the highest score is taken as the best match for a given Wikipedia article.

Currently the tool only uses movie metadata information and movie credits information. Additional information can be added to the algorithm by implementing the `matching` interface and adding the new features to `features` variable in `match.go`.

## **combine**
The `combine` command combines the movies metadata information with ratio calculations, Wikipedia links/abstract and outputs the results to a new CSV file.

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

There are a lot of incomplete/malformed inputs in the IMDB dataset. The tool considers them as "parsing errors" which are collected and output by each command. Additional information about such errors can be output when running the tool with the `-v` flag. Parsing errors do not cause the tool to exit early.

`run.sh` is a helper script that runs all four commands given the location of the zipped IMDB dataset, location of the gzipped Wikipedia dataset and a Postgres connection URI (in this exact order). The script was checked against [ShellCheck](https://www.shellcheck.net/). You must build the tool using `go build` before running this script.


# Next steps
- Add appropriate flags for all commands to pass different columns names. This would be useful when using the tool with a different version of the dataset.

- Currently the data needs to be queried directly from a Postgres table using SQL. Implementing an API would allow non-technical people to query and use the data. The API should allow users to sort films by various categories such as budget, revenue and release year.