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

func initCmd() *cobra.Command {
	var mainTemplate = `package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/autom8ter/myjson"
	_ "github.com/autom8ter/myjson/kv/badger"
	_ "github.com/autom8ter/myjson/kv/tikv"
	"github.com/autom8ter/myjson/transport/openapi"
)

var (
	provider       = flag.String("provider", "badger", "provider")
	providerParams = flag.String("provider-params", "{\"storage_path\": \"./tmp\"}", "provider params (json)")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	params := map[string]interface{}{}
	if err := json.Unmarshal([]byte(*providerParams), &params); err != nil {
		fmt.Println("failed to parse provider params: ", err.Error())
	}
	db, err := myjson.Open(ctx, *provider, params)
	if err != nil {
		fmt.Println("failed to initialize project: ", err.Error())
		return
	}
	defer db.Close(ctx)
	if err := filepath.Walk("./schema", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".yaml") {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			bits, err := io.ReadAll(f)
			if err != nil {
				return err
			}
			if err := db.ConfigureCollection(ctx, bits); err != nil {
				if err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		fmt.Println("failed to initialize project: ", err.Error())
		return
	}
	oapi, err := openapi.New(openapi.Config{
		Title:       "{{.title}}",
		Version:     "{{.version}}",
		Description: "{{.description}}",
		Port:        8080,
	})
	if err != nil {
		fmt.Println("failed to initialize project: ", err.Error())
		return
	}

	fmt.Println("starting openapi http server on port :8080")
	if err := oapi.Serve(ctx, db); err != nil {
		fmt.Println(err)
	}
}

`
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
