package audio

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeFFmpeg puts a script named "ffmpeg" at the front of PATH that prints
// script's stdout and exits 1, mimicking how real ffmpeg answers -sources:
// it writes the list and still exits non-zero. This exercises listBackend's
// actual parsing against controlled fixture text, without touching real
// hardware or the source file.
func fakeFFmpeg(t *testing.T, output string) {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\ncat <<'DEVICES_EOF'\n" + output + "\nDEVICES_EOF\nexit 1\n"
	path := filepath.Join(dir, "ffmpeg")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	old := os.Getenv("PATH")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+old)
}

// TestListBackendParsesDefaultAndDescription covers the two entry shapes
// ffmpeg actually emits: a "* "-marked default with a bracketed description,
// and an unmarked device with the same shape.
func TestListBackendParsesDefaultAndDescription(t *testing.T) {
	fakeFFmpeg(t, `Auto-detected sources for pulse:
 * default [Monitor of Built-in Audio Analog Stereo]
   alsa_input.usb-Blue_Microphones-00.mono-fallback [Blue Snowball]`)

	devices := listBackend(context.Background(), "pulse")
	if len(devices) != 2 {
		t.Fatalf("got %d devices, want 2: %+v", len(devices), devices)
	}

	if !devices[0].Default {
		t.Error("first device should be marked default")
	}
	if devices[0].Name != "default" {
		t.Errorf("name = %q, want %q", devices[0].Name, "default")
	}
	if devices[0].Label != "Monitor of Built-in Audio Analog Stereo" {
		t.Errorf("label = %q", devices[0].Label)
	}

	if devices[1].Default {
		t.Error("second device should not be marked default")
	}
	if devices[1].Name != "alsa_input.usb-Blue_Microphones-00.mono-fallback" {
		t.Errorf("name = %q", devices[1].Name)
	}
	if devices[1].Label != "Blue Snowball" {
		t.Errorf("label = %q", devices[1].Label)
	}
	for _, d := range devices {
		if d.Backend != "pulse" {
			t.Errorf("backend = %q, want pulse", d.Backend)
		}
	}
}

// TestListBackendParsesNameWithoutDescription covers devices ffmpeg lists
// with no bracketed label — the parser must not require one.
func TestListBackendParsesNameWithoutDescription(t *testing.T) {
	fakeFFmpeg(t, `Auto-detected sources for alsa:
   hw:2,0`)

	devices := listBackend(context.Background(), "alsa")
	if len(devices) != 1 {
		t.Fatalf("got %d devices, want 1: %+v", len(devices), devices)
	}
	if devices[0].Name != "hw:2,0" || devices[0].Label != "" {
		t.Errorf("got %+v", devices[0])
	}
}

// TestListBackendSkipsFailureAndHeaderLines is the case that matters most in
// practice: a backend with nothing attached (or unsupported entirely) must
// come back as zero devices, not a parsed-garbage entry or a crash.
func TestListBackendSkipsFailureAndHeaderLines(t *testing.T) {
	fakeFFmpeg(t, `Cannot list sources for alsa.`)

	devices := listBackend(context.Background(), "alsa")
	if len(devices) != 0 {
		t.Errorf("got %d devices from a failure line, want 0: %+v", len(devices), devices)
	}
}

// TestListBackendEmptyOutput guards against a nil/empty ffmpeg response
// producing a phantom device from stray whitespace.
func TestListBackendEmptyOutput(t *testing.T) {
	fakeFFmpeg(t, "")
	devices := listBackend(context.Background(), "pulse")
	if len(devices) != 0 {
		t.Errorf("got %d devices from empty output, want 0: %+v", len(devices), devices)
	}
}

func TestResolveDeviceExactMatchPasses(t *testing.T) {
	devices := []Device{{Backend: "pulse", Name: "alsa_input.usb-Blue_Microphones-00.mono-fallback"}}
	skipped, err := ResolveDevice(devices, "pulse", "alsa_input.usb-Blue_Microphones-00.mono-fallback")
	if err != nil || skipped {
		t.Errorf("ResolveDevice() = (%v, %v), want (false, nil)", skipped, err)
	}
}

// TestResolveDeviceUnknownNameRejected is the actual bug this guards
// against: on pulse, an unmatched name must be rejected with the real
// options listed, since the pulse backend itself silently falls back to the
// default source instead of erroring.
func TestResolveDeviceUnknownNameRejected(t *testing.T) {
	devices := []Device{
		{Backend: "pulse", Name: "alsa_input.usb-Blue_Microphones-00.mono-fallback"},
		{Backend: "pulse", Name: "alsa_output.pci-0000_0e_00.4.analog-stereo.monitor"},
		{Backend: "alsa", Name: "hw:2,0"}, // different backend: must not appear or count
	}
	skipped, err := ResolveDevice(devices, "pulse", "soundboard")
	if err == nil {
		t.Fatal("expected an error for an unmatched device name")
	}
	if skipped {
		t.Error("skipped = true, want false: devices were enumerated for this backend")
	}
	for _, want := range []string{"alsa_input.usb-Blue_Microphones-00.mono-fallback", "alsa_output.pci-0000_0e_00.4.analog-stereo.monitor", "pulse"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
}

// TestResolveDeviceEmptyListSkipsValidation is the escape hatch: enumeration
// is best-effort, so a backend with nothing enumerated must not block a real
// device over a parsing gap.
func TestResolveDeviceEmptyListSkipsValidation(t *testing.T) {
	skipped, err := ResolveDevice(nil, "pulse", "soundboard")
	if err != nil {
		t.Errorf("ResolveDevice() error = %v, want nil", err)
	}
	if !skipped {
		t.Error("skipped = false, want true: nothing was enumerated for this backend")
	}
}

// TestResolveDeviceWellKnownAlsaNamesAlwaysPass covers hw:*/default/
// sysdefault* names, which ALSA resolves itself and routinely won't appear
// in ffmpeg -sources alsa output at all.
func TestResolveDeviceWellKnownAlsaNamesAlwaysPass(t *testing.T) {
	devices := []Device{{Backend: "alsa", Name: "front:CARD=Generic,DEV=0"}}
	for _, name := range []string{"hw:2,0", "plughw:2,0", "default", "sysdefault:CARD=Generic"} {
		skipped, err := ResolveDevice(devices, "alsa", name)
		if err != nil || skipped {
			t.Errorf("ResolveDevice(%q) = (%v, %v), want (false, nil)", name, skipped, err)
		}
	}
}

// TestListDevicesDoesNotPanic is the end-to-end sanity check against
// whatever ffmpeg is actually installed: no backend being present is a
// normal outcome (CI has neither pulse nor alsa configured), not a bug.
func TestListDevicesDoesNotPanic(t *testing.T) {
	requireFFmpeg(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = ListDevices(ctx) // must not panic; contents depend on the host
}
