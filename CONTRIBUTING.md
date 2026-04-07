# Contributing to PulseMQ

Thank you for taking the time to contribute! This document explains how to get involved, what the coding standards are, and how the project is maintained.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Ways to contribute](#ways-to-contribute)
- [Getting the code](#getting-the-code)
- [Development setup](#development-setup)
- [Making a change](#making-a-change)
- [Testing](#testing)
- [Coding standards](#coding-standards)
- [Commit messages](#commit-messages)
- [Opening a pull request](#opening-a-pull-request)
- [Reporting bugs](#reporting-bugs)
- [Requesting features](#requesting-features)

---

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md). We are committed to a welcoming, inclusive community.

---

## Ways to contribute

- **Bug reports** — open a GitHub Issue with a minimal reproduction
- **Bug fixes** — fork → fix → open a PR (reference the issue)
- **Documentation improvements** — typos, clarity, missing examples
- **Feature requests** — open an Issue first to discuss before coding
- **New SDKs** — we welcome Node.js, Python, Java SDKs in `sdks/`
- **Performance improvements** — include benchmarks before and after

---

## Getting the code

```bash
git clone https://github.com/sneh-joshi/pulsemq
cd pulsemq
go mod download
```

Requirements:
- Go 1.24+
- Docker + Docker Compose (for integration testing)

---

## Development setup

```bash
# Run all tests
go test ./... -count=1 -timeout 90s

# Build binary
go build -o pulsemq ./cmd/server

# Start locally
./pulsemq --config config.yaml

# Start with Docker
cd docker && docker compose up -d
```

The server starts on `http://localhost:8080`. Dashboard is at `/dashboard`.

---

## Making a change

1. **Open an issue first** for non-trivial changes so we can align before you invest coding time.
2. Fork the repository and create a branch: `git checkout -b fix/my-bug` or `feat/my-feature`.
3. Make your changes — keep them focused. One concern per PR.
4. Add or update tests.
5. Run `go test ./... -count=1` and ensure all tests pass.
6. Run `go build ./...` to confirm the build is clean.
7. Open a PR.

---

## Testing

Every change must be accompanied by tests that cover the new behaviour.

```bash
# All tests
go test ./... -count=1 -timeout 90s

# Specific package
go test ./internal/broker/... -count=1 -v

# With race detector (for concurrency-related changes)
go test ./... -race -count=1 -timeout 120s
```

Test naming convention: `Test<Package>_<Behaviour>` — e.g. `TestHTTP_CreateQueue_InvalidName`.

---

## Coding standards

### Non-negotiable rules

These rules come from the project's design principles and **must not be broken**:

1. **Never bypass the storage interface** — writes always go through `StorageEngine.Append()`.
2. **Everything goes through the Broker** — HTTP handlers never call queue or storage directly.
3. **Message format is final** — only add optional fields; never rename or remove existing ones.
4. **Config structure never shrinks** — add fields freely, never remove or rename existing ones.
5. **UTC milliseconds everywhere** — use `time.Now().UnixMilli()`. No timezone conversions.
6. **ULID for all IDs** — messages, nodes, receipt handles, subscriptions.

### Style

- Run `go fmt ./...` before committing.
- Use `errors.Is` / `errors.As` for error checking — no string matching.
- Exported symbols must have Go doc comments.
- Prefer short, focused functions over large ones.
- Concurrency: prefer channels for signalling, mutexes for shared state. Document which mutex protects what.
- Avoid global mutable state; pass dependencies explicitly.

### Naming conventions

- Queue namespace and name: lowercase alphanumeric + hyphens (`^[a-z0-9][a-z0-9\-]{0,63}$`).
- All public APIs validate names at the HTTP layer — do not relax this.

---

## Commit messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

[optional body]

[optional footer: closes #123]
```

**Types:** `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`

Examples:
```
feat(scheduler): add per-queue scheduled message count endpoint
fix(handler): return 400 instead of 500 for invalid queue names
docs(api-reference): add /api/stats/summary endpoint
test(client): add TestStatsPaged covering paginated stats SDK method
```

---

## Opening a pull request

- Target the `main` branch.
- Fill in the PR template (title, problem, solution, how to test).
- Reference any related issues: `Closes #42`.
- Keep PRs focused — split large changes into smaller PRs.
- Ensure CI passes before requesting review.
- At least one maintainer approval is required to merge.

---

## Reporting bugs

Open a [GitHub Issue](https://github.com/sneh-joshi/pulsemq/issues/new) and include:

- PulseMQ version (`/health` → `version` field)
- OS / architecture
- Minimal reproduction steps (curl commands, config snippet)
- Expected vs actual behaviour
- Server logs if relevant

---

## Requesting features

Open a [GitHub Issue](https://github.com/sneh-joshi/pulsemq/issues/new) with the label `enhancement`. Describe:

- The use case / problem you are solving
- Your proposed solution (optional)
- Alternatives you considered

Large features (e.g. Phase 2 Raft clustering) will be tracked as a GitHub Project milestone.
