package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Short: "A tool for deriving movie analytics",
	}

	verboseErrors bool
)

func init() {
	rootCmd.AddCommand(ratioCmd)
	rootCmd.AddCommand(matchCmd)
	rootCmd.AddCommand(combineCmd)

	rootCmd.Flags().BoolVarP(&verboseErrors, "verbose", "v", false, "output verbose errors")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
