//go:build unix

package audio

import (
	"os"
	"syscall"
)

// interruptSignal asks ffmpeg to shut down cleanly. On Unix that is SIGINT,
// which ffmpeg handles by flushing and exiting rather than dying mid-write.
var interruptSignal os.Signal = syscall.SIGINT
