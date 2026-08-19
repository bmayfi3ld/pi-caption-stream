//go:build !unix

package audio

import "os"

// interruptSignal is the best available "stop cleanly" request. Windows has no
// real equivalent, so Kill is the fallback.
var interruptSignal os.Signal = os.Kill
