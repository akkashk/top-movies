package cmd

import (
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_readRow(t *testing.T) {
	tests := []struct {
		name        string
		in          string
		outputRow   []string
		done        bool
		wantErr     bool
		outputStats *outputStats
	}{
		{
			name:        "column names",
			in:          `id,colA,colB`,
			outputRow:   []string{"id", "colA", "colB"},
			done:        false,
			wantErr:     false,
			outputStats: makeStats("test"),
		},
		{
			name:        "data row",
			in:          `12,Bob,"Foo"`,
			outputRow:   []string{"12", "Bob", "Foo"},
			done:        false,
			wantErr:     false,
			outputStats: makeStats("test"),
		},
		{
			name:        "empty file",
			in:          ``,
			outputRow:   nil,
			done:        true,
			wantErr:     false,
			outputStats: makeStats("test"),
		},
		{
			name:        "empty fields",
			in:          `,,`,
			outputRow:   []string{"", "", ""},
			done:        false,
			wantErr:     false,
			outputStats: makeStats("test"),
		},
		{
			name:      "parse error",
			in:        `12,"bob,`,
			outputRow: []string{},
			done:      false,
			wantErr:   false,
			outputStats: &outputStats{
				inputFile: "test",
				totalRows: 0,
				rowErrors: map[int]error{
					0: fmt.Errorf("could not parse line at 0 and column 0: extraneous or missing \" in quoted-field"),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fin := csv.NewReader(strings.NewReader(test.in))
			stats := makeStats("test")
			outputRow, done, err := readRow(fin, stats)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.Equal(t, test.done, done)
				require.Equal(t, test.outputRow, outputRow)
				require.Equal(t, test.outputStats, stats)
			}
		})
	}
}

func Test_readCSV(t *testing.T) {
	tests := []struct {
		name            string
		in              string
		columnNames     []string
		expectedDataRow []string
		expectedIndices map[string]int
	}{
		{
			name: "subset of columns",
			in: `id,revenue,budget,date
1,100,10,2020`,
			columnNames:     []string{"id", "date"},
			expectedDataRow: []string{"1", "100", "10", "2020"},
			expectedIndices: map[string]int{
				"id":   0,
				"date": 3,
			},
		},
		{
			name: "all columns",
			in: `id,revenue,budget,date
1,100,10,2020`,
			columnNames:     []string{"id", "date", "budget", "revenue"},
			expectedDataRow: []string{"1", "100", "10", "2020"},
			expectedIndices: map[string]int{
				"id":      0,
				"date":    3,
				"budget":  2,
				"revenue": 1,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fin := csv.NewReader(strings.NewReader(test.in))
			parseRow := expectedRowsIndices(t, test.expectedDataRow, test.expectedIndices)
			stats := makeStats("test")
			err := readCSV(fin, stats, test.columnNames, parseRow)
			require.NoError(t, err)
			require.Empty(t, stats.rowErrors)
		})
	}

}

func expectedRowsIndices(t *testing.T, expectedRow []string, expectedIndices map[string]int) parseRowFn {
	return func(row []string, indices map[string]int, stats *outputStats) {
		// The order of elements matter in `row`
		require.Equal(t, expectedRow, row)
		require.Equal(t, expectedIndices, indices)
	}
}

func Test_decodeJSON(t *testing.T) {
	tests := []struct {
		name string
		in   string
		out  []string
	}{
		{
			name: "empty",
			in:   ``,
			out:  []string{},
		},
		{
			name: "single name",
			in:   `{"name": "Bob"}`,
			out:  []string{"Bob"},
		},
		{
			name: "single name with other fields",
			in:   `{"name": "Bob", "job": "Builder"}`,
			out:  []string{"Bob"},
		},
		{
			name: "multiple names",
			in:   `[{"name": "Bob"}, {"name": "Foo"}]`,
			out:  []string{"Bob", "Foo"},
		},
		// Known formatting issues
		{
			name: "using single quotes",
			in:   `{'name': 'Bob'}`,
			out:  []string{"Bob"},
		},
		{
			name: "using mixed quotes",
			in:   `{'name": "Bob'}`,
			out:  []string{"Bob"},
		},
		{
			name: "missing values",
			in:   `{'name": None}`,
			out:  []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ElementsMatch(t, test.out, decodeJSON(test.in))
		})
	}
}

func Test_moviesMetadata(t *testing.T) {
	year, err := getTime("2020-01-20")
	require.NoError(t, err)
	tests := []struct {
		name string
		in   *movieMetadata
		out  *movieMetadataFeatures
	}{
		{
			name: "all values present",
			in: &movieMetadata{
				title:         "Film Title",
				originalTitle: "Film Title",
				year:          year,
				production:    "Bar Studios, FooBar Productions",
			},
			out: &movieMetadataFeatures{
				title:         "film title",
				originalTitle: "film title",
				tokens:        []string{"bar studios", "foobar productions", "2020"},
			},
		},
		{
			name: "empty date",
			in: &movieMetadata{
				title:         "film title",
				originalTitle: "film title",
				production:    "bar studios, foobar productions",
			},
			out: &movieMetadataFeatures{
				title:         "film title",
				originalTitle: "film title",
				tokens:        []string{"bar studios", "foobar productions"},
			},
		},
		{
			name: "empty production",
			in: &movieMetadata{
				title:         "film title",
				originalTitle: "film title",
				year:          year,
			},
			out: &movieMetadataFeatures{
				title:         "film title",
				originalTitle: "film title",
				tokens:        []string{"2020"},
			},
		},
		{
			name: "empty title",
			in: &movieMetadata{
				year:       year,
				production: "bar studios, foobar productions",
			},
			out: &movieMetadataFeatures{
				tokens: []string{"bar studios", "foobar productions", "2020"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := test.in.feature()
			require.Equal(t, test.out.title, f.title)
			require.Equal(t, test.out.originalTitle, f.originalTitle)
			require.ElementsMatch(t, test.out.tokens, f.tokens)
		})
	}
}

func Test_moviesMetadataParseFn(t *testing.T) {
	metadataRes := make(moviesMetadata)
	parseFn := readMoviesMetadata(metadataRes)
	row := []string{"0", "2020-10-10", "film foo", `{"name": "bar productions"}`, "extra"}
	indices := map[string]int{
		"id":                   0,
		"release_date":         1,
		"production_companies": 3,
		"title":                2,
	}
	stats := makeStats("test")
	parseFn(row, indices, stats)

	require.Len(t, metadataRes, 1)
	require.Contains(t, metadataRes, "0")
	require.Equal(t, 2020, metadataRes["0"].year.Year())
	require.Equal(t, "bar productions", metadataRes["0"].production)
	require.Equal(t, "film foo", metadataRes["0"].title)
	require.Empty(t, stats.rowErrors)

	// row contains less than required entries
	row = []string{"1", "2020-10-10", "film foo"}
	parseFn(row, indices, stats)

	// Check that we did not add any new entry
	require.Len(t, metadataRes, 1)
	require.Len(t, stats.rowErrors, 1)
	require.Contains(t, stats.rowErrors[0].Error(), "row has 3 columns when at least 4 is expected")

	// Add another valid row but with invalid json for production companies
	row = []string{"2", "2020-10-10", "film bar", "foo productions"}
	parseFn(row, indices, stats)

	// Check that we added another entry
	require.Len(t, metadataRes, 2)
	require.Contains(t, metadataRes, "2")
	require.Equal(t, 2020, metadataRes["2"].year.Year())
	require.Empty(t, metadataRes["2"].production)
	require.Equal(t, "film bar", metadataRes["2"].title)
	// Check no additional errors were generated
	require.Len(t, stats.rowErrors, 1)
}

func Test_readMoviesRating(t *testing.T) {
	ratingsRes := make(ratings)
	parseFn := readMoviesRating(ratingsRes)
	indices := map[string]int{
		"movieId": 0,
		"userId":  1,
		"rating":  2,
	}
	stats := makeStats("test")

	// Unique users
	rows := [][]string{
		{"0", "U1", "5.5"},
		{"0", "U2", "5.2"},
		{"0", "U3", "5.8"},
		{"0", "U4", "5.5"},
	}
	for _, row := range rows {
		parseFn(row, indices, stats)
		stats.totalRows += 1
	}

	require.Empty(t, stats.rowErrors)
	require.EqualValues(t, 22.0, ratingsRes["0"].cumulativeRating)
	require.EqualValues(t, 4, ratingsRes["0"].numberOfRatings)
	require.Len(t, ratingsRes["0"].seenUsers, 4)
	require.Equal(t, fmt.Sprintf("%f", 5.5), ratingsRes.forID("0"))

	// Repeated users
	rows = [][]string{
		{"1", "U1", "6"},
		{"1", "U1", "7"},
		{"1", "U1", "6"},
		{"1", "U1", "3"},
	}
	for _, row := range rows {
		parseFn(row, indices, stats)
		stats.totalRows += 1
	}

	// Each duplictate row should give an error
	require.Len(t, stats.rowErrors, 3)
	require.EqualValues(t, 6.0, ratingsRes["1"].cumulativeRating)
	require.EqualValues(t, 1, ratingsRes["1"].numberOfRatings)
	require.Len(t, ratingsRes["1"].seenUsers, 1)
	require.Equal(t, fmt.Sprintf("%f", 6.0), ratingsRes.forID("1"))
}

func Test_readWiki(t *testing.T) {
	tests := []struct {
		name string
		in   string
		out  *wikiEntry
	}{
		{
			name: "empty",
			in:   ``,
			out:  nil,
		},
		{
			name: "valid",
			in: `<feed>
<doc>
<title>Wikipedia: Anarchism (film)</title>
<url>https://en.wikipedia.org/wiki/Anarchism</url>
<abstract>Anarchism is a political philosophy.</abstract>
<links>
<sublink linktype="nav"><anchor>Definition</anchor><link>https://en.wikipedia.org/wiki/Anarchism#Etymology,_terminology_and_definition</link></sublink>
</links>
</doc>
</feed>`,
			out: &wikiEntry{
				title:    "Anarchism (film)",
				url:      "https://en.wikipedia.org/wiki/Anarchism",
				abstract: "Anarchism is a political philosophy.",
				anchors:  []string{"definition"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoder := xml.NewDecoder(strings.NewReader(test.in))
			outChan := make(chan *wikiEntry, 1)
			err := readWiki(decoder, outChan)
			require.NoError(t, err)
			actualEntry := <-outChan
			require.Equal(t, test.out, actualEntry)
		})
	}
}
