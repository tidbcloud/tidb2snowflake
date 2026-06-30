package version

import (
	"fmt"
	"runtime"
)

func String() string {
	return fmt.Sprintf("%d.%d.%d %s\nGo Version: %s\nGit Ref: %s\nGitHash: %s",
		VerMajor,
		VerMinor,
		VerPatch,
		VerName,
		runtime.Version(),
		GitRef,
		GitHash)
}
