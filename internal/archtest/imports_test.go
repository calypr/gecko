package archtest

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/calypr/gecko"

type rule struct {
	pkg       string
	forbidden []string
}

var importRules = []rule{
	{pkg: "internal/integrations", forbidden: []string{"internal/server/httpapi", "internal/server/middleware", "github.com/gofiber/fiber"}},
	{pkg: "internal/access", forbidden: []string{"internal/integrations", "internal/server/httpapi", "internal/server/middleware", "github.com/gofiber/fiber"}},
	{pkg: "internal/db", forbidden: []string{"internal/server"}},
	{pkg: "internal/repo", forbidden: []string{"internal/project", "internal/storageaudit", "internal/server/httpapi", "internal/server/middleware", "github.com/gofiber/fiber"}},
	{pkg: "internal/upload", forbidden: []string{"internal/project", "internal/storageaudit", "internal/server/httpapi", "internal/server/middleware", "github.com/gofiber/fiber"}},
	{pkg: "internal/project", forbidden: []string{"internal/upload", "internal/server/httpapi", "internal/server/middleware", "github.com/gofiber/fiber"}},
	{pkg: "internal/storageaudit", forbidden: []string{"internal/project", "internal/upload", "internal/server/httpapi", "internal/server/middleware", "github.com/gofiber/fiber"}},
	{pkg: "internal/domain", forbidden: []string{"internal", "github.com/gofiber/fiber"}},
}

func TestPackageImportBoundaries(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".gocache", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		for _, importRule := range importRules {
			if !strings.HasPrefix(filepath.ToSlash(relative), importRule.pkg+"/") {
				continue
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, imported := range parsed.Imports {
				value, err := strconv.Unquote(imported.Path.Value)
				if err != nil {
					return err
				}
				target := strings.TrimPrefix(value, module+"/")
				for _, forbidden := range importRule.forbidden {
					if target == forbidden || strings.HasPrefix(target, forbidden+"/") {
						t.Errorf("%s imports forbidden package %s", filepath.ToSlash(relative), value)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
