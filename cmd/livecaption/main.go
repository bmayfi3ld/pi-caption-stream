// Command livecaption streams audio to a speech-to-text service and serves
// the resulting captions to a webpage.
//
// Audio comes from either a live capture device (the soundboard's USB output)
// or an audio file replayed at wall-clock rate, which lets the whole pipeline
// be exercised before the hardware is connected.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"livecaption/internal/cli"
	"livecaption/internal/ui"

	// Engines register themselves; main imports them for the side effect so
	// adding a provider never means editing this file's logic.
	_ "livecaption/internal/stt/deepgram"
	_ "livecaption/internal/stt/mock"
)

func main() {
	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		fmt.Fprintln(os.Stderr, "livecaption:", err)
		os.Exit(1)
	}
}

func run() error {
	kctx, c, err := cli.Parse(os.Args[1:])
	if err != nil {
		return err
	}

	term, log := ui.Setup(ui.LogConfig{
		Level:   c.LogLevel,
		Format:  c.LogFormat,
		Verbose: c.Verbose,
		Quiet:   c.Quiet,
		NoColor: c.NoColor,
	})

	// First signal cancels the context, which unwinds the pipeline cleanly.
	// A second one is the escape hatch if something refuses to stop.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		fmt.Fprintln(os.Stderr, "\nforced exit")
		os.Exit(130)
	}()

	// context.Context is an interface, so kong needs to be told the binding
	// type explicitly; the concrete values bind positionally.
	kctx.BindTo(ctx, (*context.Context)(nil))
	return kctx.Run(term, log, c.Globals)
}
