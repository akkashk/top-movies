package cmd

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// parseRowFn specifies how a row of data should be parsed for a given CSV file.
// `row` is a row of data from  a CSV file
// `indices` is a map of column name to index value of where the column value can be found in `line`
// `stats` tracks a list of errors encountered
type parseRowFn func(row []string, indices map[string]int, stats *outputStats)

// readCSV reads a CSV file. It expects the first row to contain a list of column names.
// `columnNames` provides the list of columns used by `parseRow`
func readCSV(fin *csv.Reader, stats *outputStats, columnNames []string, parseRow parseRowFn) error {
	fmt.Printf("Reading columns %v from file %s\n", columnNames, stats.inputFile)

	row, done, err := readRow(fin, stats)
	if err != nil {
		return err
	}
	if done {
		return fmt.Errorf("empty matching file")
	}
	stats.totalRows += 1

	indices := make(map[string]int, len(columnNames))
	for i, name := range row {
		for _, columnName := range columnNames {
			if name == columnName {
				indices[name] = i
			}
		}
	}

	for {
		row, done, err = readRow(fin, stats)
		if err != nil {
			return err
		}
		if done {
			break
		}

		parseRow(row, indices, stats)
		stats.totalRows += 1
	}

	return nil
}

func readRow(fin *csv.Reader, stats *outputStats) ([]string, bool, error) {
	row, err := fin.Read()
	if err != nil {
		if err == io.EOF {
			return nil, true, nil
		}

		if parseError, ok := err.(*csv.ParseError); ok {
			stats.rowErrors[stats.totalRows] = fmt.Errorf("could not parse line at %d and column %d: %v", parseError.StartLine-1, parseError.Column, parseError.Err)
			return []string{}, false, nil
		} else {
			return nil, false, fmt.Errorf("could not read record: %v", err)
		}
	}

	return row, false, err
}

// readMoviesMetadata specifies how to read a row of data from the IMDB `movies_metadata` file
func readMoviesMetadata(res moviesMetadata) parseRowFn {
	return func(row []string, indices map[string]int, stats *outputStats) {
		val := new(movieMetadata)
		var id string

		for columnName, idx := range indices {
			if idx >= len(row) {
				stats.rowErrors[stats.totalRows] = fmt.Errorf("row has %d columns when at least %d is expected", len(row), idx+1)
				return
			}

			columnValue := row[idx]
			switch columnName {
			case "id":
				id = columnValue
			case "title":
				val.title = columnValue
			case "original_title":
				val.originalTitle = columnValue
			case "production_companies":
				val.production = strings.Join(decodeJSON(columnValue), ", ")
			case "revenue":
				val.revenue = columnValue
			case "budget":
				val.budget = columnValue
			case "release_date":
				if len(columnValue) >= 4 {
					val.year = columnValue[:4]
				}
			}
		}

		if id != "" {
			res[id] = val
		}

	}
}

type moviesMetadata map[string]*movieMetadata

func (m moviesMetadata) features() moviesMetadataFeatures {
	features := make(moviesMetadataFeatures, len(m))

	for id, metadata := range m {
		features[id] = metadata.feature()
	}

	return features
}

type movieMetadata struct {
	title         string
	originalTitle string
	year          string
	production    string
	budget        string
	revenue       string
}

func (m *movieMetadata) feature() *movieMetadataFeatures {
	tokens := []string{}
	if m.production != "" {
		p := strings.ToLower(m.production)
		tokens = append(tokens, strings.Split(p, ", ")...)
	}
	if m.year != "" {
		tokens = append(tokens, m.year)
	}

	return &movieMetadataFeatures{
		title:         strings.ToLower(m.title),
		originalTitle: strings.ToLower(m.originalTitle),
		tokens:        tokens,
	}
}

type moviesMetadataFeatures map[string]*movieMetadataFeatures

type movieMetadataFeatures struct {
	title         string
	originalTitle string
	tokens        []string
}

// decodeJSON returns the list of values under the `name` token for an input JSON string.
// There were issues with the input, such as using single quotes and the use of `None` for missing values
// which throw parsing errors when using json.Unmarshal
func decodeJSON(in string) []string {
	res := []string{}
	in = strings.ReplaceAll(in, `'`, `"`)
	in = strings.ReplaceAll(in, `None`, `""`)
	dec := json.NewDecoder(strings.NewReader(in))
	readNext := false
	for {
		token, err := dec.Token()
		if err != nil {
			// Catches EOF errors
			break
		}
		switch token {
		case "name":
			readNext = true
		default:
			if readNext {
				if tokenString, ok := token.(string); ok && tokenString != "" {
					res = append(res, tokenString)
				}
				readNext = false
			}
		}
	}

	return res
}

// readMoviesCredits specifies how to read a row of data from the IMDB `credits` file
func readMoviesCredits(res moviesCredits) parseRowFn {
	return func(row []string, indices map[string]int, stats *outputStats) {
		val := new(movieCredits)
		var id string

		for columnName, idx := range indices {
			if idx >= len(row) {
				stats.rowErrors[stats.totalRows] = fmt.Errorf("row has %d columns when at least %d is expected", len(row), idx+1)
				return
			}

			columnValue := row[idx]
			switch columnName {
			case "id":
				id = columnValue
			case "cast":
				val.cast = append(val.cast, decodeJSON(columnValue)...)
			case "crew":
				val.crew = append(val.crew, decodeJSON(columnValue)...)
			}
		}

		if id != "" {
			res[id] = val
		}
	}
}

type moviesCredits map[string]*movieCredits

func (m moviesCredits) features() moviesCreditsFeatures {
	res := make(moviesCreditsFeatures)

	for id, credits := range m {
		res[id] = credits.feature()
	}

	return res
}

type movieCredits struct {
	cast []string
	crew []string
}

func (m *movieCredits) feature() movieCreditsFeatures {
	features := make([]string, 0, len(m.cast)+len(m.crew))

	for _, c := range m.cast {
		features = append(features, strings.ToLower(c))
	}

	for _, c := range m.crew {
		features = append(features, strings.ToLower(c))
	}

	return features
}

type moviesCreditsFeatures map[string]movieCreditsFeatures

type movieCreditsFeatures []string

// readMoviesRating specifies how to read a row of data from the IMDB `ratings` file
func readMoviesRating(res ratings) parseRowFn {
	return func(row []string, indices map[string]int, stats *outputStats) {
		var id string
		var rating float64
		var userID string
		var err error

		for columnName, idx := range indices {
			if idx >= len(row) {
				stats.rowErrors[stats.totalRows] = fmt.Errorf("row has %d columns when at least %d is expected", len(row), idx+1)
				return
			}

			columnValue := row[idx]

			switch columnName {
			case "movieId":
				id = columnValue
			case "userId":
				userID = columnValue
			case "rating":
				rating, err = strconv.ParseFloat(columnValue, 32)
				if err != nil {
					stats.rowErrors[stats.totalRows] = fmt.Errorf("column has value %q which cannot be converted to a float for ratings", columnValue)
					return
				}
			}
		}

		val, ok := res[id]
		if !ok {
			val = &ratingInfo{
				seenUsers: make(map[string]bool),
			}
		}

		if userID == "" {
			stats.rowErrors[stats.totalRows] = fmt.Errorf("userID is empty")
			return
		}

		if val.seenUsers[userID] {
			stats.rowErrors[stats.totalRows] = fmt.Errorf("userID %q already seen for id %q", userID, id)
			return
		}

		val.numberOfRatings += 1
		val.cumulativeRating += rating
		val.seenUsers[userID] = true

		if id != "" {
			res[id] = val
		}
	}
}

type ratingInfo struct {
	cumulativeRating float64
	numberOfRatings  float64
	seenUsers        map[string]bool
}

type ratings map[string]*ratingInfo

func (r ratings) forID(id string) string {
	val, exists := r[id]
	if !exists {
		return "N/A"
	}
	return fmt.Sprintf("%f", val.cumulativeRating/val.numberOfRatings)
}

// readMoviesRatio specifies how to read a row of data from a file containing budget to revenue ratio
func readMoviesRatio(res moviesRatios) parseRowFn {
	return func(row []string, indices map[string]int, stats *outputStats) {
		var id string
		var ratio float64
		var err error

		for columnName, idx := range indices {
			if idx >= len(row) {
				stats.rowErrors[stats.totalRows] = fmt.Errorf("row has %d columns when at least %d is expected", len(row), idx+1)
				return
			}

			columnValue := row[idx]

			switch columnName {
			case "id":
				id = columnValue
			case "ratio":
				ratio, err = strconv.ParseFloat(columnValue, 32)
				if err != nil {
					stats.rowErrors[stats.totalRows] = fmt.Errorf("column has value %q which cannot be converted to a float for ratio", columnValue)
					return
				}
			}
		}

		if id != "" {
			res[id] = ratio
		}
	}
}

type moviesRatios map[string]float64

func (m moviesRatios) forID(id string) string {
	ratio, ok := m[id]
	if !ok {
		return "N/A"
	}

	return fmt.Sprintf("%f", ratio)
}

const (
	tagTitle    = "title"
	tagAnchor   = "anchor"
	tagLink     = "link"
	tagDoc      = "doc"
	tagURL      = "url"
	tagAbstract = "abstract"
)

// readWiki specifies how to read the Wikipedia XML file
func readWiki(wikiDecoder *xml.Decoder, movieEntries chan<- *wikiEntry) error {
	defer close(movieEntries)

	var entry *wikiEntry
	for {
		token, err := wikiDecoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("could not get next token: %v", err)
		}

		switch t := token.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case tagDoc:
				// Start of a new Wikipedia entry
				if entry.isMovie() {
					movieEntries <- entry
				}
				entry = &wikiEntry{}
			case tagTitle:
				var title string
				if err := wikiDecoder.DecodeElement(&title, &t); err != nil {
					return fmt.Errorf("could not decode element: %v", err)
				}
				title = strings.TrimPrefix(title, "Wikipedia: ")
				entry.title = title
			case tagURL:
				if err := wikiDecoder.DecodeElement(&entry.url, &t); err != nil {
					return fmt.Errorf("could not decode element: %v", err)
				}
			case tagAbstract:
				if err := wikiDecoder.DecodeElement(&entry.abstract, &t); err != nil {
					return fmt.Errorf("could not decode element: %v", err)
				}
			case tagAnchor:
				var anchor string
				if err := wikiDecoder.DecodeElement(&anchor, &t); err != nil {
					return fmt.Errorf("could not decode element: %v", err)
				}
				entry.anchors = append(entry.anchors, strings.ToLower(anchor))
			}
		}
	}

	if entry != nil && entry.isMovie() {
		movieEntries <- entry
	}

	return nil
}

type wikiEntry struct {
	title    string
	url      string
	abstract string
	anchors  []string
}

func (w *wikiEntry) isMovie() bool {
	if w == nil {
		return false
	}

	if strings.Contains(strings.ToLower(w.title), "film)") {
		return true
	}

	anchorScore := anchorScore{}
	for _, anchor := range w.anchors {
		if strings.Contains(anchor, "plot") {
			anchorScore.plot = 1
		} else if strings.Contains(anchor, "cast") {
			anchorScore.cast = 1
		} else if strings.Contains(anchor, "production") {
			anchorScore.production = 1
		} else if strings.Contains(anchor, "reception") {
			anchorScore.reception = 1
		} else if strings.Contains(anchor, "release") {
			anchorScore.release = 1
		}
	}

	if anchorScore.score() > 3 {
		return true
	}

	return false
}

type anchorScore struct {
	plot       int
	cast       int
	production int
	reception  int
	release    int
}

func (a anchorScore) score() int {
	return a.plot + a.cast + a.production + a.reception + a.release
}

// readWikiMatches specifies how to read a row of data from a file containing matched wikipedia data
func readWikiMatches(res wikiMatches) parseRowFn {
	return func(row []string, indices map[string]int, stats *outputStats) {
		val := new(wikiMatch)
		var id string

		for columnName, idx := range indices {
			if idx >= len(row) {
				stats.rowErrors[stats.totalRows] = fmt.Errorf("row has %d columns when at least %d is expected", len(row), idx+1)
				return
			}

			columnValue := row[idx]
			switch columnName {
			case "id":
				id = columnValue
			case "url":
				val.url = columnValue
			case "abstract":
				val.abstract = columnValue
			case "score":
				val.score = columnValue
			}
		}

		if id != "" {
			res[id] = val
		}
	}
}

type wikiMatches map[string]*wikiMatch

type wikiMatch struct {
	url      string
	abstract string
	score    string
}

type outputStats struct {
	inputFile string
	totalRows int
	// Each row will only have one error
	rowErrors map[int]error
}

func makeStats(inputFile string) *outputStats {
	return &outputStats{
		inputFile: inputFile,
		rowErrors: make(map[int]error),
	}
}

func (o *outputStats) String() string {
	sBuilder := new(strings.Builder)
	fmt.Fprintf(sBuilder, "A total of %d rows were parsed from %s. %d errors.\n",
		o.totalRows,
		o.inputFile,
		len(o.rowErrors),
	)

	if verboseErrors {
		sBuilder.WriteString("Parse errors:\n")
		for i, err := range o.rowErrors {
			fmt.Fprintf(sBuilder, "error on row %d: %v\n", i, err)
		}
	} else {
		sBuilder.WriteString("Run tool with -v flag to get verbose error outputs.\n")
	}

	return sBuilder.String()
}
