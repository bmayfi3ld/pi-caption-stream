package audio

import (
	"encoding/binary"
	"math"
)

// silenceFloor is reported in place of -Inf for an empty or all-zero buffer,
// so callers (the auto-pause gate) can compare it numerically against a
// threshold without a special case for silence.
const silenceFloor = -100.0

// fullScale16 is the largest magnitude a 16-bit signed sample can reach, used
// as the 0 dBFS reference.
const fullScale16 = 32768.0

// RMSDBFS returns the RMS level of a 16-bit little-endian PCM buffer in dBFS
// (0 = full scale, negative below). Returns silenceFloor for an empty or
// all-zero buffer rather than -Inf, so callers can compare numerically.
func RMSDBFS(pcm []byte) float64 {
	n := len(pcm) / 2 // ignore a trailing odd byte
	if n == 0 {
		return silenceFloor
	}

	var sumSquares float64
	for i := 0; i < n; i++ {
		s := int16(binary.LittleEndian.Uint16(pcm[2*i:]))
		v := float64(s)
		sumSquares += v * v
	}

	rms := math.Sqrt(sumSquares / float64(n))
	if rms == 0 {
		return silenceFloor
	}

	db := 20 * math.Log10(rms/fullScale16)
	if db < silenceFloor {
		return silenceFloor
	}
	return db
}
