set dotenv-load := true

run *args:
    go run ./cmd/livecaption {{args}}
