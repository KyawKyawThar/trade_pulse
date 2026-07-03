package version

// These are overridden at build time, e.g.
//
//	go build -ldflags "-X github.com/tradepulse/shared/version.Commit=$(git rev-parse --short HEAD)"

const (
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
