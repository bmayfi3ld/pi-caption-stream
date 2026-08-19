package audio

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// Device is one capture input the tool can be pointed at.
type Device struct {
	Backend string // "pulse" or "alsa"
	Name    string // pass this to --device
	Label   string // human description
	Default bool
}

// ListDevices enumerates capture inputs for every backend. Backends that are
// unavailable are skipped silently: on a PipeWire box `-sources alsa` often
// fails outright, and that is not an error worth reporting.
func ListDevices(ctx context.Context) []Device {
	var all []Device
	for _, backend := range []string{"pulse", "alsa"} {
		all = append(all, listBackend(ctx, backend)...)
	}
	return all
}

func listBackend(ctx context.Context, backend string) []Device {
	// ffmpeg writes the source list to stderr and exits non-zero; both are
	// expected, so only the output matters.
	out, _ := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-sources", backend).CombinedOutput()

	var devices []Device
	for line := range strings.SplitSeq(string(out), "\n") {
		trimmed := strings.TrimSpace(line)
		// Entries look like:  * default [Description]
		//                       alsa_input.usb-... [HyperX DuoCast]
		if trimmed == "" || strings.HasPrefix(trimmed, "Auto-detected") ||
			strings.HasPrefix(trimmed, "Cannot list") || strings.HasSuffix(trimmed, ":") {
			continue
		}
		isDefault := strings.HasPrefix(trimmed, "*")
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "*"))
		if trimmed == "" {
			continue
		}
		name, label := trimmed, ""
		if i := strings.Index(trimmed, " ["); i >= 0 {
			name = strings.TrimSpace(trimmed[:i])
			label = strings.TrimSuffix(strings.TrimPrefix(trimmed[i+1:], "["), "]")
		}
		if name == "" {
			continue
		}
		devices = append(devices, Device{
			Backend: backend,
			Name:    name,
			Label:   label,
			Default: isDefault,
		})
	}
	return devices
}

// isWellKnownDeviceName covers names ALSA resolves itself rather than
// ffmpeg -sources enumerating: hw:*/plughw:* card,device pairs and the
// default/sysdefault aliases routinely don't appear in the parsed list at
// all, so treating a miss as a rejection would block a real device over an
// enumeration gap rather than a typo.
func isWellKnownDeviceName(name string) bool {
	if name == "default" {
		return true
	}
	for _, prefix := range []string{"hw:", "plughw:", "sysdefault"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// ResolveDevice checks a requested --device name against what ListDevices
// found for backend, so a typo is caught before ffmpeg opens anything. This
// matters most for pulse: PipeWire's pulse-compat layer silently falls back
// to the default source for an unknown device name instead of erroring, so
// the probe-read fail-fast in device.go never catches it — the capture just
// starts on the wrong input.
//
// skipped reports that nothing was enumerated for backend at all, so no
// judgement could be made. Enumeration is best-effort (it shells out to
// ffmpeg and parses free-form text), and refusing to start on unconfirmed
// information would be worse than the bug this guards against — callers
// should warn and proceed rather than treat skipped as an error.
func ResolveDevice(devices []Device, backend, name string) (skipped bool, err error) {
	if isWellKnownDeviceName(name) {
		return false, nil
	}

	var names []string
	for _, d := range devices {
		if d.Backend != backend {
			continue
		}
		if d.Name == name {
			return false, nil
		}
		names = append(names, d.Name)
	}
	if len(names) == 0 {
		return true, nil
	}
	if len(names) > 8 {
		return false, fmt.Errorf("device %q not found on %s backend (%d devices available; run `livecaption devices` to list them)",
			name, backend, len(names))
	}
	return false, fmt.Errorf("device %q not found on %s backend; available: %s", name, backend, strings.Join(names, ", "))
}
