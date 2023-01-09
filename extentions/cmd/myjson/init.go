package main

import (
	"fmt"
	"os"
	"text/template"

	_ "embed"

	"github.com/Masterminds/sprig/v3"
	"github.com/autom8ter/myjson/testutil"
	"github.com/spf13/cobra"
)

//go:embed templates/main.go.tmpl
var mainTemplate string

func initCmd() *cobra.Command {
	var (
		projectPath string
		version     string
		title       string
		description string
	)
	cmd := &cobra.Command{
		Use:   "init",
		Short: "create a new myjson project",
		Run: func(_ *cobra.Command, _ []string) {
			os.MkdirAll(projectPath, 0755)
			os.MkdirAll(projectPath+"/schema", 0755)
			{
				f, _ := os.Create(fmt.Sprintf("%s/schema/account.yaml", projectPath))
				defer f.Close()
				f.Write([]byte(testutil.AccountSchema))
			}
			{
				f, _ := os.Create(fmt.Sprintf("%s/schema/user.yaml", projectPath))
				defer f.Close()
				f.Write([]byte(testutil.UserSchema))
			}
			{
				f, _ := os.Create(fmt.Sprintf("%s/schema/task.yaml", projectPath))
				defer f.Close()
				f.Write([]byte(testutil.TaskSchema))
			}
			{
				tmpl, err := template.New("").Funcs(sprig.FuncMap()).Parse(mainTemplate)
				if err != nil {
					fmt.Println("failed to initialize project: ", err.Error())
					return
				}
				f, _ := os.Create(fmt.Sprintf("%s/main.go", projectPath))
				defer f.Close()
				err = tmpl.Execute(f, map[string]interface{}{
					"title":       title,
					"version":     version,
					"description": description,
				})
				if err != nil {
					fmt.Println("failed to initialize project: ", err.Error())
					return
				}
			}

			fmt.Printf("new project created: %v\n", projectPath)
		},
	}
	cmd.Flags().StringVarP(&projectPath, "path", "p", ".", "path to project directory")
	cmd.Flags().StringVarP(&title, "title", "t", "change me", "title of project")
	cmd.Flags().StringVarP(&description, "description", "d", "change me", "description of project")
	cmd.Flags().StringVarP(&version, "version", "v", "v0.0.0", "version of project")
	return cmd
}
