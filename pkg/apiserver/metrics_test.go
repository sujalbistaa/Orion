package apiserver

import "github.com/sujalbistaa/orion/pkg/telemetry"

func newTestMetrics() *telemetry.Metrics { return telemetry.New() }
