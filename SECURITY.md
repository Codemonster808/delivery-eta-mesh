# Security Policy

This is a personal portfolio / demo project (an event-driven ETA recomputation
mesh built against a local MiniStack AWS emulator). It is not a production
service, does not process real customer data, and is not actively monitored
for security reports on a service-level-agreement basis.

## Reporting a Vulnerability

If you spot a security issue (e.g. a dependency vulnerability, a credential
committed by mistake, an insecure default), please open a GitHub issue on
this repository or contact the maintainer directly via GitHub. There is no
bug bounty program and no guaranteed response time, but reports are welcome
and will be looked at.

## Scope Notes

- Dependencies are scanned automatically via Dependabot (`.github/dependabot.yml`)
  and `pip-audit` in CI (`.github/workflows/ci.yml`, `security` job).
- Local development uses MiniStack (a local AWS emulator) with fake
  credentials (`AWS_ACCESS_KEY_ID=test`) — these are not real secrets and
  are safe to have in `.env.example`.
- Do not use this repository's code as-is in a production environment
  without an independent security review.
