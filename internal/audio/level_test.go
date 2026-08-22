package audio

import (
	"encoding/binary"
	"math"
	"testing"
)

// squareWave builds n samples alternating between +amp and -amp, whose RMS is
// exactly amp, making the expected dBFS easy to state.
func squareWave(n int, amp int16) []byte {
	pcm := make([]byte, n*2)
	for i := 0; i < n; i++ {
		v := amp
		if i%2 == 1 {
			v = -amp
		}
		binary.LittleEndian.PutUint16(pcm[2*i:], uint16(v))
	}
	return pcm
}

func TestRMSDBFS(t *testing.T) {
	const tolerance = 0.1

	cases := []struct {
		name string
		pcm  []byte
		want float64
	}{
		{"silence", make([]byte, 3200), silenceFloor},
		{"empty buffer", nil, silenceFloor},
		{"full-scale square wave", squareWave(100, 32767), 0},
		{"half-amplitude square wave", squareWave(100, 16384), -6.02},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RMSDBFS(tc.pcm)
			if math.Abs(got-tc.want) > tolerance {
				t.Errorf("RMSDBFS(%s) = %v, want ~%v", tc.name, got, tc.want)
			}
		})
	}
}

// TestRMSDBFSIgnoresTrailingOddByte guards against an off-by-one panic when a
// buffer isn't a whole number of 16-bit samples.
func TestRMSDBFSIgnoresTrailingOddByte(t *testing.T) {
	pcm := squareWave(10, 32767)
	pcm = append(pcm, 0x7f) // dangling odd byte
	got := RMSDBFS(pcm)
	want := RMSDBFS(squareWave(10, 32767))
	if got != want {
		t.Errorf("trailing odd byte changed the result: got %v, want %v", got, want)
	}
}
