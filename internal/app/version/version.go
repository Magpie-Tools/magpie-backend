package version

// Default values are overridden at build time via -ldflags in the Dockerfile.
// Keep these lower-case so ldflags can set them without exporting internals.
var (
	buildVersion = "dev"
	builtAt      = "unknown"
)

// Info represents the running backend build metadata.
type Info struct {
	BuildVersion string `json:"buildVersion"`
	BuiltAt      string `json:"builtAt"`
}

// Get returns the current backend build metadata.
func Get() Info {
	return Info{
		BuildVersion: buildVersion,
		BuiltAt:      builtAt,
	}
}
