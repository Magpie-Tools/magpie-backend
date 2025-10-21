package version

type Info struct {
	Version string `json:"version"`
	BuiltAt string `json:"built_at,omitempty"`
}

var (
	buildVersion = "dev"
	builtAt      = ""
)

func BuildVersion() string {
	return buildVersion
}

func BuiltAt() string {
	return builtAt
}

func GetInfo() Info {
	return Info{
		Version: BuildVersion(),
		BuiltAt: BuiltAt(),
	}
}
