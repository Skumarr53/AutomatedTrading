---
description: Plan-to-Build transition detection - Auto-activate context-aware rules when implementing plans
globs: ["**/*.plan.md", "**/.cursor/plans/**", "**/plans/**"]
alwaysApply: false
priority: 1000
---

# Plan-to-Build Transition: ACTIVATE

## Activation: IMMEDIATE

**ACTIVATE** when:
- `.plan.md` detected → **EXECUTE** plan context loading
- User says `build`, `implement plan`, `execute plan` → **FORCE** Engineer mode
- Build button clicked → **INJECT** implementation keywords

## Rule Stack: MANDATORY LOAD ORDER

1. **ACTIVATE Engineer Mode** (BUILD persona) - ALWAYS when plan detected
2. **EXTRACT** keywords from plan → **LOAD** matching task modes
3. **PRESERVE** plan context → **INJECT** `code`, `implement`, `refactor`, `optimize`

## Keyword → Rule Mapping (AUTO-ACTIVATE)

| Plan Contains | **LOAD** |
|---------------|----------|
| `review`, `pr`, `feedback` | `code_review.mdc` |
| `design`, `architecture`, `system` | `system_design.mdc` |
| `performance`, `optimize`, `speed` | `performance.mdc` |
| `security`, `vulnerability`, `audit` | `security.mdc` |
| `deploy`, `infrastructure`, `terraform` | `deployment.mdc` |
| `research`, `investigate`, `explore` | `research.mdc` |
| `brainstorm`, `prototype` | `brainstorming.mdc` |

## Execution Protocol

**ON Build click WITHOUT user message:**
```
AUTO-INJECT: "Implement the plan. MANDATORY: Include [DESIGN RATIONALE], [CRITICAL THINKING], and [LEARNING] sections. Code without understanding = REFUSAL."
```

**ON plan detection (Learning Checklist):**
1. **SCAN** plan content → extract keywords
2. **LOAD** base stack: `prime_directives`, `system_context`, `cognitive_modes`
3. **MATCH** keywords → **LOAD** `task_modes/*.mdc`
4. **VERIFY** mandatory learning sections are prioritized in output structure.

## Mandatory Implementation Checklist
- [ ] `[DESIGN RATIONALE]` - Why this approach over alternatives
- [ ] `[CRITICAL THINKING]` - Pressure-test Q&A (scale/failure)
- [ ] `[LEARNING]` - Key insight to internalize
- [ ] **REFUSAL**: If any section is missing, STOP and redo.

## Fallback: DEFAULT TO ENGINEER

No plan detected + Build clicked → **FORCE** Engineer mode + `alwaysApply: true` rules only.

## Override: EXPLICIT MODE SELECTION

User can force modes: `"Implement plan with code_review and performance mode"` → **OVERRIDE** auto-detection.

## Plan File Naming (MANDATORY)

**RULE**: All plan files MUST use timestamp format `{description}_{YYYYMMDD}_{HHMMSS}.plan.md` and save to project `.cursor/plans/` if task is project-related, else `~/.cursor/plans/`. **REFUSE** generic names or hash suffixes.

## Plan-Driven Execution Protocol (NON-NEGOTIABLE)

### I. Primary Source of Truth
- **Path:** `*/plans/`
- **Convention:** `*.plan` or `*.plan.md`
- **Role:** You are strictly an **Executor**. Always locate and read the most recent `.plan` file in the directory above before writing any code.
- **Mandate:** Follow the specific Phase and Task logic defined in these files without deviation.

### II. Strict Implementation Guardrails

**No Refactoring:**
- Do NOT redesign architectures found in `.plan` files.
- If a plan specifies an "Infrastructure" pattern, do NOT suggest alternative patterns.
- Plans are architectural contracts, not suggestions.

**Dependency Locking:**
- Use ONLY the libraries and versions mentioned in the plan.
- Do NOT upgrade dependencies unless explicitly requested.
- Do NOT add dependencies not specified in the plan.

**Conflict Resolution:**
- If the code in the editor contradicts the logic in the `.plan` file, **STOP** and ask for clarification.
- Do NOT "fix" the architecture to match the code.
- Do NOT assume the plan is outdated without explicit user confirmation.

### III. Token & Context Management

**Plan Reference Protocol:**
- Use the `@` symbol to reference the specific plan file (e.g., `@phase_7_infrastructure_ops_19a736a1.plan`) to ensure the model focuses only on relevant instructions.
- Keep responses focused purely on the current phase's implementation to avoid redundant token generation.
- When multiple plans exist, reference the most recent one explicitly.

### IV. Verification Step (MANDATORY)

**Plan Alignment Check:**
Every PR or code block generated MUST end with a brief "Plan Alignment" verification:

```markdown
## Plan Alignment Verification
- ✅ Does this match Phase X requirements? Yes/No
- ✅ Are all defined interfaces respected? Yes/No
- ✅ Are dependencies locked as specified? Yes/No
- ✅ Does architecture match plan without deviation? Yes/No
```

**Refusal Protocol:**
- If ANY alignment check fails, **STOP** and request clarification.
- Do NOT proceed with implementation until alignment is confirmed.
