# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.x (current) | ✅ |
| < 1.0 | ❌ |

## Reporting a Vulnerability

**Please do not open a public GitHub Issue for security vulnerabilities.**

If you discover a security vulnerability, report it privately so it can be patched before public disclosure.

**How to report:**

1. Open a [GitHub Security Advisory](https://github.com/sneh-joshi/pulsemq/security/advisories/new) (preferred).
2. Or send an email with details — include "PulseMQ Security" in the subject line.

**What to include:**

- A description of the vulnerability
- Steps to reproduce (minimal example)
- The potential impact
- If known, a suggested fix

**What to expect:**

- Acknowledgement within **48 hours**
- A timeline for a fix within **7 days** for critical issues, **30 days** for others
- Credit in the changelog and release notes (unless you prefer to remain anonymous)

## Security Model

PulseMQ is designed for **trusted networks** (internal services, private VPCs). The threat model for Phase 1 (single-node) is:

| Threat | Mitigation |
|--------|-----------|
| Unauthorised API access | Static API key via `X-Api-Key` header (`auth.enabled: true`) |
| Request body flooding | `MaxBodyMiddleware` — requests capped at `queue.max_message_size_kb` |
| Rate-limit abuse | Per-IP token-bucket rate limiter (`RateLimitMiddleware`) |
| Path traversal in queue names | `validName()` + `namespace.ValidateName()` at HTTP layer — rejects non-lowercase-alphanumeric names |
| Replay attacks | Receipt handles are single-use ULIDs — re-ACKing returns 410 Gone |

## Known Limitations

- **No TLS built-in** — terminate TLS at a reverse proxy (nginx, Caddy, Traefik) in front of PulseMQ.
- **Single static API key** — all authenticated callers share the same key. Per-namespace keys are planned for Phase 2.
- **No IP allowlisting built-in** — use your network/firewall for IP-level access control.
- **No audit log** — request logging is informational only; structured logs are written to stdout.

## Known Vulnerabilities Being Tracked

| ID | Dependency | Severity | Status |
|----|-----------|----------|--------|
| GO-2026-4923 | bbolt v1.4.3 | Medium | No fix version available upstream yet. Will update as soon as a patched bbolt release is available. |

## Dependency Security

Run `go list -json -m all | nancy sleuth` or `govulncheck ./...` to check for known vulnerabilities in dependencies.

```bash
go install golang.org/x/vuln/cmd/govulncheck@latest
govulncheck ./...
```

