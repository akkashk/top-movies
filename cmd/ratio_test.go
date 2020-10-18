package cmd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// func Test_readLine(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		in      string
// 		wantErr bool
// 	}{{
// 		name:    "valid",
// 		in:      `name,title,revenue`,
// 		wantErr: false,
// 	}}

// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			// Setup test
// 			fin := csv.NewReader(strings.NewReader(test.in))
// 			fout := csv.NewWriter(bytes.NewBuffer(nil))
// 			stats := &outputStats{
// 				budgetRevenueErrors: make([]error, 0),
// 				parseErrors:         make([]error, 0),
// 			}

// 			// Run test
// 			line, done, err := readLine(fin, stats)

// 			// Check test result
// 			if test.wantErr {
// 				require.Error(t, err)
// 				require.Contains(t, err.Error(), test.err)
// 			} else {
// 				require.Equal(t, test.rIdx, rIdx)
// 				require.Equal(t, test.bIdx, bIdx)
// 			}
// 		})
// 	}
// }

func Test_processHeader(t *testing.T) {

}

func Test_processRows(t *testing.T) {

}

func Test_outputStats(t *testing.T) {
	tests := []struct {
		name        string
		in          *outputStats
		verboseFlag bool
		out         string
	}{{
		name: "empty",
		in:   &outputStats{},
		out: `A total of 0 out of 0 rows analysed and saved in . 0 parse errors and 0 revenue/budget errors.
Run tool with -v flag to get verbose error outputs.
`,
	},
		{
			name: "no errors with counts",
			in: &outputStats{
				outputPath: "~/custom/analyse.csv",
				totalRows:  30,
				parsedRows: 30,
			},
			out: `A total of 30 out of 30 rows analysed and saved in ~/custom/analyse.csv. 0 parse errors and 0 revenue/budget errors.
Run tool with -v flag to get verbose error outputs.
`,
		},
		{
			name: "errors",
			in: &outputStats{
				outputPath:          "output",
				totalRows:           32,
				parsedRows:          30,
				parseErrors:         []error{fmt.Errorf("parse error")},
				budgetRevenueErrors: []error{fmt.Errorf("b/r error")},
			},
			out: `A total of 30 out of 32 rows analysed and saved in output. 1 parse errors and 1 revenue/budget errors.
Run tool with -v flag to get verbose error outputs.
`,
		},
		{
			name: "errors with verbose flag",
			in: &outputStats{
				outputPath:          "output",
				totalRows:           32,
				parsedRows:          30,
				parseErrors:         []error{fmt.Errorf("parse error")},
				budgetRevenueErrors: []error{fmt.Errorf("b/r error")},
			},
			verboseFlag: true,
			out: `A total of 30 out of 32 rows analysed and saved in output. 1 parse errors and 1 revenue/budget errors.
Parse errors:
error 1: parse error
Budget/Revenue errors:
error 1: b/r error
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verboseErrors = test.verboseFlag
			require.Equal(t, test.out, fmt.Sprint(test.in))
		})
	}
}
