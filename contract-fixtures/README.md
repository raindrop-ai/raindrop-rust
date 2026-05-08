# Contract Fixtures (vendored)

This directory is a **vendored mirror** of the cross-language Contract v1
golden corpus. Source-of-truth lives in `raindrop-workshop`:

- Upstream path: `raindrop-workshop/contract/fixtures/v1/`
- Upstream test: `raindrop-workshop/tests/contract-corpus.test.ts`

It is checked in here so that `cargo test` is hermetic — tests must not
depend on a sibling repo being present on disk. Whenever the upstream
corpus changes, re-run `scripts/sync_contract_fixtures.sh` (which simply
`rsync`s `raindrop-workshop/contract/fixtures/v1/` into this directory)
and commit the diff.

## Cross-language drift-detection

Each JSON file under `v1/<endpoint>/<scenario>.json` is one canonical wire
payload that all four implementations of the Contract v1 wire must agree
on:

1. **Workshop HTTP server** — accepts the body and returns 2xx (or 4xx
   for the explicitly invalid scenarios).
2. **`@raindrop-ai/core` zod schemas** — `LiveEventSchema`,
   `TrackBodySchema`, `TrackPartialEventSchema`, etc. parse the body
   without errors.
3. **Python pydantic mirrors** (Phase G3) — `pydantic.BaseModel`
   subclasses in the Python SDK deserialize the body identically.
4. **Rust serde mirrors** (this crate, Phase G4) — `serde::Deserialize`
   structs under `src/contract/v1/` deserialize the body identically.

The Rust half of the drift detection is `tests/contract_corpus.rs`. When
any single implementation drifts from the others, that language's
contract test fails on the offending fixture and points directly at the
drift.

## Layout

```
v1/
├── meta.json                  describes the corpus (endpoints, validity, schemas)
├── live/                      POST /v1/live
├── track/                     POST /v1/events/track
├── track-partial/             POST /v1/events/track_partial
├── traces/                    POST /v1/traces (OTLP/JSON)
└── replay/                    Workshop ↔ user-repo replay adapter wire
```

## Validity

`meta.json::validity` lists fixtures that are intentionally invalid. The
Rust contract test asserts that the Rust serde mirrors (or the
post-deserialize `validate_*` functions) reject them. Anything not listed
is valid.

`meta.json::schemaSkip` lists fixtures whose JSON shape is not yet
representable in the strict schema (e.g. `track-partial/with-trace-id-only`
lacks `event_id` because Workshop accepts trace-only attach today). These
are loaded but skipped from the strict-deserialize pass.

## Adding or updating a fixture

The corpus is **not edited here**. To add or update a fixture:

1. Land the change in `raindrop-workshop/contract/fixtures/v1/` and
   confirm the TS contract corpus test (`bun test contract-corpus`)
   passes.
2. From this crate's root, run `scripts/sync_contract_fixtures.sh` to
   pull the new state into `contract-fixtures/v1/`.
3. Run `cargo test --test contract_corpus`. If the Rust serde mirrors
   need a corresponding update to keep parity, that update is part of
   the same PR.
