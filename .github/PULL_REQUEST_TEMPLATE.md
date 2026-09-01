## Summary

<!-- What does this PR change and why? -->

## Component(s) touched

- [ ] Python (ingestion / transformation / orchestration / serving / utils)
- [ ] Java worker (`src/worker/`)
- [ ] Infra (docker-compose, ECS task def)
- [ ] Docs / specs / ADRs

## Checklist

- [ ] `make test` passes locally (pytest + `mvn test`)
- [ ] `pre-commit run --all-files` passes
- [ ] Updated `docs/specs/` and/or `docs/adr/` if behavior or a documented
      threshold (`WATERMARK_MINUTES`, `SALT_BUCKETS`, MAE target, etc.) changed
- [ ] No secrets or `.env` changes committed

## Test Plan

<!-- How did you verify this change? -->
