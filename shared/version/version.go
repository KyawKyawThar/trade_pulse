package version

// These are overridden at build time (-X only works on vars, not consts), e.g.
//
//	go build -ldflags "-X trade_pulse/shared/version.Commit=$(git rev-parse --short HEAD)"

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

type Info struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

func GetInfo() Info {
	return Info{
		Version,
		Commit,
		Date,
	}
}
