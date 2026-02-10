---
description: OPS PROTOCOL - Debugging, Maintenance, and Cleanup standards.
globs: []
alwaysApply: true
---

# 🛠️ Ops Protocol: Execute & Maintain

## I. Troubleshooting (The Diagnostic Loop)
**Rule**: "Diagnose -> Explain -> Fix -> Verify"
1.  **Automated State Check**: Use `/database-inspect` or `/mcp-tool-usage` to autonomously verify system health.
2.  **Root Cause Hypothesis**: State it clearly based on automated findings.
3.  **The Fix**: A single, executable script (no manual steps).

### MCP Tool Integration for Troubleshooting
**When debugging database/metric issues**:
- **ClickHouse**: Use `query_clickhouse` MCP tool to inspect metrics, validate queries, check table stats
- **Qdrant**: Use `query_collection` MCP tool to debug semantic search, inspect embeddings
- **Postgres**: Use Postgres MCP tools to query agent checkpoints, inspect state transitions
- **InfluxDB**: Use `query_metrics` MCP tool to check trading metrics, validate data quality
- **Prometheus**: Use Prometheus MCP tools to query metrics, validate alert thresholds

**When debugging CI/CD or Git issues**:
- **GitLab**: Use GitLab MCP tools (`gitlab_get_merge_request`, `gitlab_list_merge_requests`) to check pipeline status, MR conflicts

**Preference**: Always use MCP tools over manual CLI commands when available. MCP provides better integration and automatic result formatting.

## II. Artifact Hygiene (Cleanup)
- **Automatic**: Delete intermediate debug scripts (`/tmp/*`, `*.test.py`) immediately after success.
- **Project Root**: MUST remain clean. Move configs to `config/`, scripts to `scripts/`.
- **Command**: `cleanup now` triggers aggressive removal of non-git-tracked files.
- **Plan Files**: Save as `{description}_{YYYYMMDD}_{HHMMSS}.plan.md` in project `.cursor/plans/` if project-related, else `~/.cursor/plans/`. **REFUSE** generic names or hash suffixes.

## III. Verification Gates
Before marking a task complete:
1.  **Lint**: `ruff check .`
2.  **Test**: `pytest` (at least 1 happy path).
3.  **Run**: Execute the code/script.

## IV. Error Handling Pattern
The ONLY acceptable error pattern:
```python
try:
    await unsafe_operation()
except SpecificError as e:
    logger.error("Context", error=e, payload=data)
    raise DomainError("Human readable failure") from e
```

## V. Production Paranoia Mode (The "No-Ghost" Policy)
- Assume 100k RPM from day one.
- Every function must have: metrics, logging, error handling.
- Design for observability first.
- Include failure mode analysis.
- Cost estimate required for cloud resources.

**Resilience Requirements**:
- **Edge Cases**: Identify and handle input extremes, null states, and race conditions.
- **Dependency Guarding**: Minimize external service failure rates via aggressive retry logic, timeouts, and circuit breakers.
- **Silent Failures = Incomplete Work**: Every failure must be traceable. Implement structured logging with context. If an error occurs and the logs don't explain exactly *where* and *why*, the code is broken.

## VI. Debug Logging Protocol
**FORBIDDEN**: Writing to debug log files (`debug.log` or similar).
- **MANDATORY**: Use terminal logging only (`logger.info()`, `logger.debug()`, `logger.error()`).
- **Rationale**: User tests in debug mode with terminal tracking; file logging is redundant.
- **Exception**: None. All debugging must use standard logging framework.
- **Always**: editor breakpoint debugging; pdb is discouraged.

## VII. Battle-Test Protocol (50-Line Rule)
**Rule**: Big code is a liability until proven functional.
- **Trigger**: Any change exceeding 50 lines or modifying core architecture.
- **Requirement**: Mandatory "Battle-Test" section must be provided.
- **Content**:
  - Specific debug script, `curl` command, or log-tailing instruction.
  - Visual: Show "Expected Output" vs "Failure State".
- **Objective**: User should never ask "how do I run this?" after a major push.

## VIII. VS Code Integrated Debugging Protocol (MANDATORY)
**Rule**: "Always provide VS Code debugging instructions when asked about testing."

### Required Components
1. **VS Code `launch.json` configurations** for each component (Python, Ray, FastAPI, etc.).
2. **Step-by-step debugging instructions** with explicit breakpoint locations (file:line).
3. **Variable Inspection Guide**: What to watch in the debugger (variables, call stack, state).
4. **Interactive Debugging Scenarios**: Example sessions for each feature.

### Implementation Checklist
- [ ] Create/update `.vscode/launch.json` with tailored debug configurations.
- [ ] List specific file paths and line numbers for breakpoints.
- [ ] Define "Variables to Watch" with expected vs. failure values.
- [ ] Provide a step-by-step walkthrough (Trigger -> Break -> Inspect -> Step -> Validate).
- [ ] Include debugging tips and shortcuts (e.g., `F5`, `F10`, `Shift+F5`).
- [ ] Show example debugging sessions for complex logic.

**Failure State**: If debugging instructions are missing, generic, or ignore `launch.json`, code is NOT production-ready. The user must be able to hit `F5` and verify state integrity immediately.
