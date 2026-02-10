---
description: SKILLS OVERRIDE - Skills take priority over .cursor/rules
globs: ["**/*"]
alwaysApply: true
priority: 10000
---

# 🔄 Skills Override Protocol

## Rule Priority

**SKILLS > RULES**: Skills installed via skills.sh take **priority** over all `.cursor/rules/` files.

### Priority Hierarchy

```
1. SKILLS (Priority: 10000) - HIGHEST
   ├─ ~/.agents/skills/*
   ├─ ~/.cursor/skills/*
   └─ ~/.claude-code/skills/*

2. USER PROMPT (Priority: 9999)

3. RULES (Priority: 1-9998)
   ├─ prime_directives.mdc (Priority: 9999)
   ├─ master_index.mdc (Priority: 9999)
   └─ All other .cursor/rules/*
```

## Activation Protocol

When a **skill is active** for the current context:
1. **Skill instructions** are executed first
2. **Rules** provide fallback behavior
3. **Prime directives** remain as guardrails (non-negotiable)

## Skill Detection

Skills auto-detect based on:
- File type (`.py`, `.rs`, `.dockerfile`, etc.)
- Directory structure (`k8s/`, `infrastructure/`, etc.)
- Keyword context (`"debug"`, `"test"`, `"deploy"`, etc.)
- Project patterns (ML, DevOps, Trading, etc.)

## Integration Notes

- **Skills extend, not replace** - Skills add domain expertise
- **Prime directives preserved** - Quality, learning, innovation remain
- **Mode switching still works** - Plan/Build/Ops/Vibe modes respect skills
- **Learning sections required** - Even with skills, learning is sacred

## Skill Inventory

View installed skills:
```bash
npx skills list --global
```

Update skills:
```bash
npx skills update
```

Add new skills:
```bash
cd ~/.skills
./install_tier2.sh  # For additional recommended skills
```
