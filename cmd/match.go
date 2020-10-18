package cmd

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/spf13/cobra"
)

const (
	tagTitle    = "title"
	tagAnchor   = "anchor"
	tagLink     = "link"
	tagDoc      = "doc"
	tagURL      = "url"
	tagAbstract = "abstract"
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
	wikiPath := args[0]

	wikiFile, err := os.Open(wikiPath)
	if err != nil {
		return err
	}
	defer wikiFile.Close()

	wikiDecoder := xml.NewDecoder(wikiFile)
	movieEntries := make(chan *wikiEntry, 1000)

	// Read movies dataset
	metadataPath := args[1]
	metadataFile, err := os.Open(metadataPath)
	if err != nil {
		return err
	}
	defer metadataFile.Close()

	metadataMatching, err := readMoviesMetadata(csv.NewReader(bufio.NewReader(metadataFile)))
	if err != nil {
		return err
	}

	creditsPaths := args[2]
	creditsFile, err := os.Open(creditsPaths)
	if err != nil {
		return err
	}
	defer creditsFile.Close()

	creditsMatching, err := readMoviesCredits(csv.NewReader(bufio.NewReader(creditsFile)))
	if err != nil {
		return err
	}

	go func() {
		if err := readWiki(wikiDecoder, movieEntries); err != nil {
			fmt.Printf("error reading wiki dataset: %v", err)
		}
	}()

	features := []matching{
		metadataMatching,
		creditsMatching,
	}

	results := map[string]*matchResult{}
	for entry := range movieEntries {
		mostRelevantIDs := make(chan string, 100)
		var wg sync.WaitGroup

		for _, feature := range features {
			go feature.mostRelevant(entry, mostRelevantIDs, &wg)
			wg.Add(1)
		}

		go func() {
			wg.Wait()
			close(mostRelevantIDs)
		}()

		var maxScore float64
		var bestID string
		for id := range mostRelevantIDs {
			var score float64
			for _, feature := range features {
				score += feature.relevance(entry, id)
			}

			score = score / float64(len(features))
			if score > maxScore {
				bestID = id
				maxScore = score
			}
			// fmt.Println(entry.title)
			// fmt.Println("ID:", id, "Score:", score)
		}

		if maxScore > 0 {
			if currentRes, ok := results[bestID]; ok {
				if currentRes.score > maxScore {
					continue
				}
			}
			results[bestID] = &matchResult{
				score: maxScore,
				url:   entry.url,
			}

			fmt.Printf("%d films matched\n", len(results))
			if len(results)%100 == 0 {
				writeRes(results)
			}
		}
	}

	fout, err := os.Create("output_matching.csv")
	if err != nil {
		return err
	}
	defer fout.Close()
	writer := csv.NewWriter(fout)
	if err := writer.Write([]string{"id,url,score"}); err != nil {
		return err
	}
	for id, res := range results {
		writer.Write([]string{id, res.url, fmt.Sprintf("%f", res.score)})
	}
	writer.Flush()

	return writer.Error()
}

func writeRes(res map[string]*matchResult) {
	fout, _ := os.Create("output_matching.csv")
	defer fout.Close()
	writer := csv.NewWriter(fout)
	writer.Write([]string{"id,url,score"})
	for id, res := range res {
		writer.Write([]string{id, res.url, fmt.Sprintf("%f", res.score)})
	}
	writer.Flush()
}

type matching interface {
	// mostRevelant returns a list of most relevant ids
	mostRelevant(*wikiEntry, chan<- string, *sync.WaitGroup)
	// revevance calculates a score given a id
	relevance(*wikiEntry, string) float64
}

func readMoviesMetadata(metadata *csv.Reader) (matching, error) {
	stats := &outputStats{}
	line, done, err := readLine(metadata, stats)
	if err != nil {
		return nil, err
	}
	if done {
		return nil, fmt.Errorf("empty metadata file")
	}

	var idIdx, prodCompanyIdx, releaseDateIdx, titleIdx int
	for i, heading := range line {
		switch heading {
		case "id":
			idIdx = i
		case "title":
			titleIdx = i
		case "production_companies":
			prodCompanyIdx = i
		case "release_date":
			releaseDateIdx = i
		}
	}

	res := &moviesMetadata{
		data: map[string]*moviesMetadataInfo{},
	}

	for {
		line, done, err = readLine(metadata, stats)
		if err != nil {
			return nil, err
		}
		if done {
			break
		}

		if idIdx >= len(line) || prodCompanyIdx >= len(line) || releaseDateIdx >= len(line) || titleIdx >= len(line) {
			continue
		}

		tokens := decodeJSON(line[prodCompanyIdx])
		if date := line[releaseDateIdx]; len(date) > 4 {
			date = date[:4]
			tokens = append(tokens, date)
		}

		if len(tokens) > 0 {
			res.data[line[idIdx]] = &moviesMetadataInfo{
				title:  strings.ToLower(line[titleIdx]),
				tokens: tokens,
			}
		}
	}

	return res, nil
}

type info struct {
	Name string `json:"name"`
}

type moviesMetadata struct {
	data map[string]*moviesMetadataInfo
}

type moviesMetadataInfo struct {
	title  string
	tokens []string
}

func (m *moviesMetadata) mostRelevant(e *wikiEntry, out chan<- string, wg *sync.WaitGroup) {
	for id, md := range m.data {
		var tokenScore float64
		for _, token := range md.tokens {
			if strings.Contains(e.abstract, token) {
				tokenScore += 1
			}
		}
		tokenScore = tokenScore / float64(len(md.tokens))

		var titleScore float64
		if md.title != "" && strings.Contains(e.title, md.title) {
			titleScore = 1
		}

		score := (0.5 * tokenScore) + (0.5 * titleScore)
		if score >= 0.5 {
			out <- id
		}
	}

	wg.Done()
}

func (m *moviesMetadata) relevance(e *wikiEntry, id string) float64 {
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
		titleScore = 1
	}

	score := (0.5 * tokenScore) + (0.5 * titleScore)

	return score
}

func readMoviesCredits(credits *csv.Reader) (matching, error) {
	stats := &outputStats{}
	line, done, err := readLine(credits, stats)
	if err != nil {
		return nil, err
	}
	if done {
		return nil, fmt.Errorf("empty credits file")
	}

	var idIdx, castIdx, crewIdx int
	for i, heading := range line {
		switch heading {
		case "id":
			idIdx = i
		case "cast":
			castIdx = i
		case "crew":
			crewIdx = i
		}
	}

	res := &moviesCredits{
		data: map[string][]string{},
	}

	for {
		line, done, err = readLine(credits, stats)
		if err != nil {
			return nil, err
		}
		if done {
			break
		}

		if idIdx >= len(line) || castIdx >= len(line) || crewIdx >= len(line) {
			continue
		}

		tokens := []string{}
		tokens = append(tokens, decodeJSON(line[castIdx])...)
		tokens = append(tokens, decodeJSON(line[crewIdx])...)

		if len(tokens) > 0 {
			res.data[line[idIdx]] = tokens
		}
	}

	return res, nil
}

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
				t, _ := dec.Token()
				if tokenString, ok := t.(string); ok {
					res = append(res, tokenString)
				}
				readNext = false
			}
		}
	}

	return res
}

type moviesCredits struct {
	data map[string][]string
}

func (m *moviesCredits) mostRelevant(e *wikiEntry, out chan<- string, wg *sync.WaitGroup) {
	for id, md := range m.data {
		var score, total float64
		for _, token := range md {
			if strings.Contains(e.abstract, token) {
				score += 1
			}
			total += 1
		}

		if score/total > 0 {
			out <- id
		}
	}

	wg.Done()
}

func (m *moviesCredits) relevance(e *wikiEntry, id string) float64 {
	md, ok := m.data[id]
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

	return score / total
}

type matchResult struct {
	score float64
	url   string
}

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
				entry.title = strings.ToLower(title)
			case tagURL:
				var url string
				if err := wikiDecoder.DecodeElement(&url, &t); err != nil {
					return fmt.Errorf("could not decode element: %v", err)
				}
				entry.url = strings.ToLower(url)
			case tagAbstract:
				var abstract string
				if err := wikiDecoder.DecodeElement(&abstract, &t); err != nil {
					return fmt.Errorf("could not decode element: %v", err)
				}
				entry.abstract = strings.ToLower(abstract)
			case tagAnchor:
				var anchor string
				if err := wikiDecoder.DecodeElement(&anchor, &t); err != nil {
					return fmt.Errorf("could not decode element: %v", err)
				}
				entry.anchors = append(entry.anchors, strings.ToLower(anchor))
			}
		}
	}
	return nil
}

type wikiEntry struct {
	title    string
	url      string
	abstract string
	anchors  []string
}

// func (w *wikiEntry) String() string {
// 	return fmt.Sprintf("movie %s with anchors %v. See %s", w.title, w.anchors, w.url)
// }

func (w *wikiEntry) isMovie() bool {
	if w == nil {
		return false
	}

	if strings.Contains(strings.ToLower(w.title), "film") {
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
