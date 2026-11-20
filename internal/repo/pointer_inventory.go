package repo

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
)

type DataPointer struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

const maxInventoryPointerBytes = 4096

func WalkDataPointers(ctx context.Context, repository *gogit.Repository, tree *object.Tree, emit func(DataPointer) error) error {
	var walk func(*object.Tree, string) error
	walk = func(tree *object.Tree, prefix string) error {
		for _, entry := range tree.Entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if prefix == "" && (entry.Name == "META" || entry.Name == "CONFIG") {
				continue
			}
			name := path.Join(prefix, entry.Name)
			switch entry.Mode {
			case filemode.Dir:
				child, err := tree.Tree(entry.Name)
				if err != nil {
					return fmt.Errorf("read tree %s: %w", name, err)
				}
				if err := walk(child, name); err != nil {
					return err
				}
			case filemode.Regular, filemode.Deprecated, filemode.Executable:
				size, err := repository.Storer.EncodedObjectSize(entry.Hash)
				if err != nil {
					return fmt.Errorf("read blob size for %s: %w", name, err)
				}
				if size < 100 || size > maxInventoryPointerBytes {
					continue
				}
				file, err := tree.File(entry.Name)
				if err != nil {
					return fmt.Errorf("read blob %s: %w", name, err)
				}
				reader, err := file.Reader()
				if err != nil {
					return fmt.Errorf("open blob %s: %w", name, err)
				}
				data, err := io.ReadAll(io.LimitReader(reader, maxInventoryPointerBytes+1))
				_ = reader.Close()
				if err != nil {
					return fmt.Errorf("read blob %s: %w", name, err)
				}
				if len(data) > maxInventoryPointerBytes {
					continue
				}
				pointer := ParseLFSPointer(data)
				if pointer != nil && pointer.Size >= 0 {
					if err := emit(DataPointer{Path: name, SHA256: strings.TrimPrefix(pointer.OID, "sha256:"), Size: pointer.Size}); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	return walk(tree, "")
}
