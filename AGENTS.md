# AGENTS.md

## Cursor Cloud specific instructions

This is the Raindrop Rust SDK (crate `raindrop`, async/tokio). Standard commands and
E2E details are in `README.md` and `.github/workflows/ci.yml`; follow those. Notes that
are not obvious:

- **MSRV is 1.88** (`Cargo.toml` `rust-version`). The toolchain in this environment is
  a current stable that satisfies it.
- **`--all-features` requires OpenSSL dev headers.** The `native-tls` feature links
  against system OpenSSL, so `libssl-dev` + `pkg-config` must be present (already
  installed in the VM). The default feature set is `rustls-tls`, so a plain
  `cargo build` / `cargo test` needs no system OpenSSL.
- Core verification (all offline): `cargo build --all-targets --all-features`,
  `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test --all-features --lib`, `cargo test --all-features --doc`,
  `cargo test --all-features --test '*'` (integration tests use `wiremock`).
- The `tests/e2e.rs` / `e2e_real_agent.rs` suites and the `e2e_verify` example need
  `RAINDROP_WRITE_KEY` + `RAINDROP_DASHBOARD_TOKEN`; they auto-skip when unset.
- Examples (`cargo run --example track_ai|begin_finish|manual_spans`) run without
  credentials — the client no-ops network delivery when no write key is set.
