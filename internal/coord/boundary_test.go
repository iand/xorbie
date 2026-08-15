package coord

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// modulePath is the import path of the module that currently holds the core.
const modulePath = "github.com/probe-lab/zikade"

// coreDirs are the module relative directories that make up the neutral Kademlia
// core. Every Go file below them, tests included, is subject to the import
// boundary enforced by TestCoreImportBoundary.
var coreDirs = []string{
	"internal/coord",
	"internal/kadtest",
	"internal/nettest",
	"internal/tiny",
}

// nonCoreDirs are directories below a core directory that are not part of the
// core. There are none: this module holds only the core.
var nonCoreDirs = []string{}

// allowedModules are the third party module paths the core may import. A path
// matches when it equals an entry or is below it.
var allowedModules = []string{
	"github.com/ipfs/go-libdht",
	"go.opentelemetry.io/otel",
}

// pendingImports records the imports that breach the boundary today, keyed by
// the module relative directory of the package that makes them. It is a ratchet:
// an import listed here is reported but tolerated, an import not listed here
// fails the test, and an entry that no longer occurs fails the test so the list
// cannot go stale. Emptying it is the definition of done for the decoupling
// work, after which both the map and this comment should be deleted.
var pendingImports = map[string][]string{
	"internal/coord": {
		"github.com/stretchr/testify/require",
		"github.com/stretchr/testify/suite",
	},
	"internal/coord/query": {
		"github.com/stretchr/testify/require",
	},
	"internal/coord/routing": {
		"github.com/stretchr/testify/assert",
		"github.com/stretchr/testify/require",
	},
	"internal/coord/brdcst": {
		"github.com/stretchr/testify/assert",
		"github.com/stretchr/testify/require",
	},
	"internal/kadtest": {
		"github.com/stretchr/testify/assert",
		"github.com/stretchr/testify/require",
	},
	"internal/tiny": {
		"github.com/stretchr/testify/assert",
	},
}

// TestCoreImportBoundary asserts that the neutral Kademlia core depends on the
// standard library and allowedModules alone. It is the mechanical statement of
// the boundary that lets the core be lifted into its own module: an import of
// kadt, pb or any other Amino package ties the core to a single protocol, and an
// import of a module outside allowedModules puts that module in the dependency
// graph of everyone who consumes the core.
func TestCoreImportBoundary(t *testing.T) {
	root := moduleRoot(t)

	seen := map[string]bool{}

	for _, dir := range coreDirs {
		walkDir(t, filepath.Join(root, dir), func(path string) {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				t.Fatalf("relative path of %s: %v", path, err)
			}
			rel = filepath.ToSlash(rel)

			pkgDir := filepath.ToSlash(filepath.Dir(rel))
			if !inCore(pkgDir) {
				return
			}

			fset := token.NewFileSet()
			parsed, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", rel, err)
			}

			for _, spec := range parsed.Imports {
				imp := strings.Trim(spec.Path.Value, `"`)
				if importAllowed(imp) {
					continue
				}

				pos := fset.Position(spec.Pos())
				if slices.Contains(pendingImports[pkgDir], imp) {
					seen[pkgDir+" "+imp] = true
					continue
				}

				t.Errorf("%s:%d: %s imports %s, which is outside the core boundary", rel, pos.Line, pkgDir, imp)
			}
		})
	}

	// An entry that no longer occurs means the boundary has tightened and the
	// ratchet must follow, otherwise the list stops describing the work left.
	var stale []string
	for pkgDir, imports := range pendingImports {
		for _, imp := range imports {
			if !seen[pkgDir+" "+imp] {
				stale = append(stale, fmt.Sprintf("%s: %s", pkgDir, imp))
			}
		}
	}
	if len(stale) > 0 {
		slices.Sort(stale)
		t.Errorf("pendingImports lists imports that no longer occur, remove them:\n\t%s", strings.Join(stale, "\n\t"))
	}

	var outstanding []string
	for pkgDir, imports := range pendingImports {
		for _, imp := range imports {
			outstanding = append(outstanding, fmt.Sprintf("%s: %s", pkgDir, imp))
		}
	}
	if len(outstanding) > 0 {
		slices.Sort(outstanding)
		t.Logf("%d imports remain outside the core boundary:\n\t%s", len(outstanding), strings.Join(outstanding, "\n\t"))
	}
}

// walkDir calls fn for every Go file below dir, skipping directories excluded
// from the core and any testdata directory.
func walkDir(t *testing.T, dir string, fn func(path string)) {
	t.Helper()

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return fs.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		fn(path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
}

// importAllowed reports whether the core may import path. Standard library
// packages and the modules named in allowedModules are always permitted, as is
// any package that is itself part of the core.
func importAllowed(path string) bool {
	if isStdlib(path) {
		return true
	}

	for _, mod := range allowedModules {
		if path == mod || strings.HasPrefix(path, mod+"/") {
			return true
		}
	}

	if rel, ok := strings.CutPrefix(path, modulePath+"/"); ok {
		return inCore(rel)
	}

	return false
}

// isStdlib reports whether path names a standard library package. The first
// segment of every other import path is a domain and so contains a dot.
func isStdlib(path string) bool {
	first, _, _ := strings.Cut(path, "/")
	return !strings.Contains(first, ".")
}

// inCore reports whether the module relative directory dir belongs to the core.
func inCore(dir string) bool {
	for _, exc := range nonCoreDirs {
		if dir == exc || strings.HasPrefix(dir, exc+"/") {
			return false
		}
	}
	for _, root := range coreDirs {
		if dir == root || strings.HasPrefix(dir, root+"/") {
			return true
		}
	}
	return false
}

// moduleRoot returns the directory holding the go.mod of the module under test.
func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod found above %s", dir)
		}
		dir = parent
	}
}
