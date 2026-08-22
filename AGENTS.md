# Agent instructions

## Do not run the binary

Never start the app — no `go run ./cmd/livecaption ...`, no `just run`, no running a built
binary, not in the background, not "just for a second". It does not exit cleanly: the process
holds the listener on `:8080` after the shell command returns, so the next run fails to bind and
the leftover process has to be killed by hand.

This applies to backgrounded runs too. Backgrounding hides the problem rather than avoiding it.

Assume the user already has an instance running when you need one.

## Verify without running

- `go build ./...`
- `go test ./...` — `internal/web/server_test.go` spins servers up and down inside the test
  process, which is safe; that is the place to add HTTP-level coverage.
- Static assets can be checked directly (`ffprobe` for the media in `internal/web/static/`).
- Browser behaviour in `internal/web/static/index.html` cannot be verified from here. Say so and
  leave it to the user rather than starting a server to try.

## Dev Work
- Do not do work inside of worktrees in harness specific folders (eg: .claude/**), instead do all work in this local project directly
- Do not create commits, wait for the user to review the changes
