package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func main() {
	cmd := &cobra.Command{
		Use:   "myjson",
		Short: "myjson is a relational json database",
	}
	cmd.AddCommand(initCmd())
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
	}
}
