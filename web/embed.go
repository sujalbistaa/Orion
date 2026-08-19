// Package web embeds the built console so orion-server ships as a single
// binary. The build tag means a server can be built without the console
// (`go build`), and with it once `make web-build` has produced web/dist.
package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed all:dist
var dist embed.FS

// ConsoleFS returns the embedded console assets, or false when the build did
// not include them.
func ConsoleFS() (http.FileSystem, bool) {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		return nil, false
	}
	// An empty dist directory means the console was never built; serving it
	// would give every page a blank screen rather than an honest warning.
	if entries, err := fs.ReadDir(sub, "."); err != nil || len(entries) == 0 {
		return nil, false
	}
	return http.FS(sub), true
}
