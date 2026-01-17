# AutomatedTrading Project Rules

## Project-Specific Rules

This directory contains **only** project-specific rules for the AutomatedTrading project.

| Rule | Purpose | Triggers |
|------|---------|----------|
| `trading_context.mdc` | Risk-first trading, ML predictions, Fyers API, backtesting | Trading-related files (`src/trading_logic/**/*`, `**/*trading*.py`, etc.) |
| `CURSOR_WORKFLOW_RULES.mdc` | Cursor IDE workflow optimization, model selection, context management | Always active (`alwaysApply: true`) |

## Global Rules

All global rules are loaded from `~/.cursor/rules/` and apply to this project:

- `system_env.mdc` - CachyOS, GNOME, fish, paru, uv, podman
- `socratic_mentor.mdc` - Principal Architect persona
- `python_standards.mdc` - Python code quality (triggers on `*.py`)
- `cleanup_protocol.mdc` - Deferred cleanup
- `self_improvement.mdc` - Dynamic rule enhancement
- `troubleshooting_protocol.mdc` - Diagnostic-first troubleshooting
- `script_standards.mdc` - Script execution standards

See `~/.cursor/rules/master_index.mdc` for the complete global rules registry.

## Rule Loading

Cursor automatically:
1. Loads global rules from `~/.cursor/rules/` (always active)
2. Loads project-specific rules from this directory
3. Merges both sets

**No duplicates** - global rules are the single source of truth.

## Verification

Run `~/.scripts/verify_cursor_rules.sh` to check for duplicates and validate glob patterns.
