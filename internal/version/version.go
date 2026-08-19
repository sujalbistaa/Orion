// Package version carries build metadata injected at link time.
package version

import "runtime"

var (
	Version   = "dev"
	Commit    = "none"
	BuildDate = "unknown"
)

// Info is reported by every binary's --version flag and by the API server's
// /healthz and /api/v1/cluster endpoints, so an operator can confirm which
// build is actually running on each control-plane replica and node.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
	Platform  string `json:"platform"`
}

func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}
}

func (i Info) String() string {
	return "orion " + i.Version + " (" + i.Commit + ") " + i.GoVersion + " " + i.Platform
}
