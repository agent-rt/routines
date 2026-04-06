# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Routines** is a deterministic workflow orchestration engine for AI agents and developers. Core value: turn probabilistic LLM execution into auditable, repeatable "muscle memory" routines. Think "geek-edition Shortcuts" + "AI cerebellum."

Stack: Rust single binary, rusqlite for audit storage, YAML DSL for workflow configs, native MCP server for AI agent integration.

## Build & Run Commands

```bash
# Initialize project (not yet created)
cargo new routines --bin

# Standard Rust workflow
cargo build                        # debug build
cargo build --release              # production binary
cargo test                         # run all tests
cargo test <test_name>             # run single test
cargo clippy -- -D warnings        # lint
cargo fmt --check                  # format check

# CLI usage (once built)
./target/debug/routines run <namespace>:<name>   # execute a routine
./target/debug/routines trigger <name>           # trigger by name (human testing)
./target/debug/routines serve                    # start MCP daemon
./target/debug/routines log <run_id>             # audit log for a run
./target/debug/routines link <file.yml> --namespace <ns> --name <n>  # symlink to hub
```

## Architecture

### Core Data Flow
```
YAML Config → Parser → Step Graph → Executor → SQLite Audit Log
                                        ↓
                              stdout/stderr capture → secret masking → DB write
```

### Key Paths
- `~/.routines/hub/<namespace>/<name>.yml` — global routine registry
- `~/.routines/data.db` — rusqlite audit database (workflow_runs + step_logs tables)
- `~/.routines/.env` — secrets store (never committed, never logged plaintext)

### Module Boundaries (planned)
- **parser**: YAML DSL → typed `Routine` / `Step` structs
- **executor**: runs steps sequentially, captures output, writes audit trail
- **step types**: `cli` (subprocess), `api` (reqwest), `mcp` (MCP client protocol)
- **context**: state dictionary passing `{{ step_id.stdout }}` between steps
- **audit**: rusqlite write layer with atomic per-step logging
- **secrets**: loads `.env` + keychain, provides masking filter for all I/O
- **server**: MCP endpoint exposing `list_routines` / `run_routine` tools
- **cli**: clap-based entry point routing to above modules

### YAML DSL Template Syntax
Variables use `{{ step_id.stdout }}` and `{{ secrets.KEY }}` interpolation. Steps are linear sequences; the engine halts the entire run on first `Err`.

### Security Invariants
- `secrets.*` values are redacted to `[REDACTED_SECRET]` before any SQLite write or MCP response
- `strict_mode: true` in YAML header enables destructive-command interception (regex-matched CLI args)

## Development Phases (from PLAN.md)
0. DSL spike: write real complex YAML by hand, validate mental model
1. Engine: YAML parser + CLI step executor + stdout/stderr capture
2. Persistence: rusqlite schema + synchronous per-step audit writes
3. Connectivity: `api` + `mcp` step types, then wrap self as MCP server
4. Audit UX: `routines log` TUI output, optional Tauri dashboard
