package version

import (
	"fmt"
	"runtime"
)

// Version is the semver of tidb2snowflake
type Version struct {
	major int
	minor int
	patch int
	name  string
}

// NewVersion creates a Version object
func NewVersion() *Version {
	return &Version{
		major: VerMajor,
		minor: VerMinor,
		patch: VerPatch,
		name:  VerName,
	}
}

// Name returns the alternative name of Version
func (v *Version) Name() string {
	return v.name
}

// SemVer returns Version in semver format
func (v *Version) SemVer() string {
	return fmt.Sprintf("%d.%d.%d", v.major, v.minor, v.patch)
}

// String converts Version to a string
func (v *Version) String() string {
	return fmt.Sprintf("%s %s\n%s", v.SemVer(), v.name, NewBuildInfo())
}

// Build is the info of building environment
type Build struct {
	GitHash   string `json:"gitHash"`
	GitRef    string `json:"gitRef"`
	GoVersion string `json:"goVersion"`
}

// NewBuildInfo creates a Build object
func NewBuildInfo() *Build {
	return &Build{
		GitHash:   GitHash,
		GitRef:    GitRef,
		GoVersion: runtime.Version(),
	}
}

// String converts Build to a string
func (v *Build) String() string {
	return fmt.Sprintf("Go Version: %s\nGit Ref: %s\nGitHash: %s", v.GoVersion, v.GitRef, v.GitHash)
}
