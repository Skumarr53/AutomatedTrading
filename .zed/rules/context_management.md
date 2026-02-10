---
description: CONTEXT MANAGEMENT - Scoping rules, token budget awareness, and dynamic context discovery.
globs: []
alwaysApply: true
priority: 500
---

# Context Management Protocol

## I. Dynamic Context Discovery (Blog Best Practice)
**Rule**: "Provide fewer details up front, let agent pull relevant context on its own."

### Initial Query Protocol
- **First Response**: Provide minimal context, let agent request specific files/functions
- **Agent Requests**: When agent asks for specific context, provide it fully
- **Exception**: Implementation phase requires full context (see `plan_build_transition.mdc`)

### Context Pulling Strategy
| Scenario | Action |
|----------|--------|
| User asks "how does X work?" | **DON'T** include full file upfront. **DO** search codebase and provide relevant snippets |
| User says "implement Y" | **DO** include full relevant files (implementation needs complete context) |
| User asks "refactor Z" | **DO** include Z file + dependencies, **DON'T** include entire codebase |
| Multi-file edit requested | **DO** include all affected files, **DON'T** include unrelated modules |

## II. Context Scoping Rules

### When to Limit Context
**MANDATORY** context limits apply when:
- Codebase > 10k files (e.g., `sentinal` project)
- Multi-file edits spanning > 5 files
- Token budget approaching limits (128k context window)
- User explicitly requests "minimal context"

### File Inclusion Patterns
**ALWAYS Include**:
- Source code files directly referenced (`src/**/*.py`)
- Configuration files (`config/**/*.yaml`, `pyproject.toml`)
- Test files for referenced modules (`tests/**/test_*.py`)
- Documentation for referenced APIs (`docs/**/*.md`)

**NEVER Include** (handled by `.cursorignore`):
- Build artifacts (`__pycache__/`, `dist/`, `build/`)
- Dependencies (`.venv/`, `node_modules/`)
- Infrastructure state (`*.tfstate`, `.terraform/`)
- Lock files (`uv.lock`, `poetry.lock`)

### Token Budget Awareness
| Context Size | Strategy |
|--------------|----------|
| < 10k tokens | Include full files |
| 10k-50k tokens | Include relevant functions/classes, summarize rest |
| 50k-100k tokens | Summarize architecture, include only critical code paths |
| > 100k tokens | **REFUSE** - Request user to narrow scope or split task |

## III. Large Codebase Handling

### Project-Specific Rules
**For `sentinal` project** (291+ git files, extensive infrastructure):
- **Default**: Limit context to `src/` directory unless infrastructure explicitly requested
- **Infrastructure queries**: Include `infrastructure/terraform/` selectively (not entire directory)
- **Agent queries**: Prefer semantic search over full file inclusion

**For `AutomatedTrading` project**:
- **Default**: Include `src/` and `config/` directories
- **Exclude**: `data/`, `model_artifacts/`, `mlruns/` (too large, not code)

## IV. Context Summarization Protocol

### When to Summarize vs Include
**Summarize** when:
- File > 500 lines and only partially relevant
- Multiple similar files (e.g., multiple executors)
- Infrastructure configs (terraform, k8s) - summarize structure, include only relevant resources

**Include Full** when:
- File < 200 lines
- Directly referenced in user query
- Implementation phase (per `plan_build_transition.mdc`)

### Summarization Format
```
[FILE SUMMARY: path/to/file.py]
Purpose: [What this file does]
Key Functions: [list 3-5 critical functions]
Dependencies: [list external imports]
[Include only relevant function if > 500 lines]
```

## V. Multi-File Edit Context Management

### Blast Radius Calculation
Before multi-file edits:
1. **Identify** all affected files (direct + transitive dependencies)
2. **Count** total lines of code
3. **Estimate** token usage (~4 tokens per line)
4. **If > 50k tokens**: Request user confirmation or split into phases

### Incremental Context Loading
For large refactors:
- **Phase 1**: Load core files, implement changes
- **Phase 2**: Load dependent files, update integrations
- **Phase 3**: Load test files, verify compatibility

## VI. Context Refresh Protocol

### When to Re-read Files
- File modified since last read (check git status)
- User explicitly requests "refresh context"
- Agent detects stale information (e.g., function signature mismatch)

### Cache Strategy
- **Cache**: File contents for duration of conversation
- **Invalidate**: On file modification or explicit refresh request
- **Never Cache**: Secrets, credentials, environment-specific configs

## VII. Refusal Protocol for Context Overflow

**STOP & REFUSE** if:
- Requested context exceeds 100k tokens
- User asks for "entire codebase" without justification
- Multi-file edit spans > 10 files without phase planning

**Action**: Request user to narrow scope or split task into phases.
