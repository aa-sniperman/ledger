package migrations

import "embed"

// Files contains all embedded up migrations for the service.
//
//go:embed *.up.sql
var Files embed.FS
