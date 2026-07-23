# Claude Code instructions for tickstore

- Read SPEC.md first. Work one milestone at a time; do not skip ahead.
- Go 1.22+, standard library first; allowed deps: github.com/coder/websocket
  (the maintained successor to nhooyr.io/websocket) or gorilla/websocket,
  clickhouse-go/v2, prometheus/client_golang, gopkg.in/yaml.v3. Ask before
  adding anything else.
- No floats for prices or sizes. Fixed-point int64 everywhere.
- Every exported type and function gets a doc comment.
- Table-driven tests. Parser changes require golden-file tests.
- Keep packages internal/ except cmd/.
- Small commits, one logical change each, imperative commit messages.
- After each milestone, write a short summary of design decisions made
  and open questions, so the author can review and adjust.
