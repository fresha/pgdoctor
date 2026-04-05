# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.0] - 2026-04-05

### Added

- **Streaming output**: results print as each check completes instead of batching, with category headers preserved.
- **Per-check timing**: visible with `--detail verbose` or `--detail debug`. Total timing always shown in summary.
- **`SeveritySkip`**: checks that fail to run (timeout, permission error) are reported as `[SKIP]` with the reason, instead of aborting the entire run.
- **`Filter()` function**: public API to filter checks by ID or category before execution.
- **`ReportHandler` type** and **`Collect()` helper**: clean callback-based API for consuming check results.
- **`Options` struct**: replaces long parameter list in `Run()` for better readability and extensibility.
- **`statement_timeout`**: uses PostgreSQL-level timeout per query instead of Go context timeout, keeping the connection healthy after slow queries.

### Changed

- **Default detail level** changed from `summary` to `brief` (shows subchecks and details for non-passing checks).
- **`Run()` API redesign**: accepts `Options` struct with callback, no error return. Check errors become `SeveritySkip` reports.
- **`SeverityOK.String()`** returns `"pass"` instead of `"ok"` (4-char alignment: pass/warn/fail/skip).
- **`vacuum-settings` check** no longer skips entirely without instance metadata — runs all non-RAM-dependent checks (scale factors, cost settings, critical misconfigurations).

### Removed

- `Run()` no longer accepts `only`/`ignored` parameters directly — use `Filter()` before calling `Run()`.

## [0.5.0] - 2026-03-19

### Added

- Extended `InstanceMetadata` with high availability, storage autoscaling, security, and protection fields.

## [0.4.0] - 2026-03-12

### Added

- Configurable timeout thresholds for session-settings check via `check.Config`.
- Dynamic role discovery for session-settings check.
- CI pipeline with linting, testing, and codegen validation.
- Auto-release workflow triggered by CHANGELOG updates.
- Documentation site with interactive index page.

### Changed

- Improved Go idioms and fixed type stuttering across the codebase.
- Mobile-responsive documentation pages.

### Removed

- Removed dev-indexes check (Fresha-specific convention, moved to contrib).

## [0.3.0] - 2026-02-21

### Added

- Interactive documentation site (`docs/index.html`) with interlinking between checks.

### Removed

- Removed dev-indexes check (Fresha-specific convention).

## [0.2.0] - 2026-02-20

### Added

- `CategoryPatterns` for check categorization.
- Extended `InstanceMetadata` fields for richer instance-aware checks.

## [0.1.0] - 2026-03-10

### Added

- Initial open-source release of pgdoctor.
- 26 PostgreSQL health checks covering configuration, indexes, schema, vacuum, and performance.
- CLI with text and JSON output formats.
- Preset system (`all` and `triage`) for check filtering.
- Shell completion for bash, zsh, fish, and powershell.
- Configurable timeout thresholds for session-settings check.
- Dynamic role discovery for session-settings check.
