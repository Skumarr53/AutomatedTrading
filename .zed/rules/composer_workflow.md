---
description: COMPOSER WORKFLOW - When to use Composer vs Chat vs Plan mode for optimal efficiency.
globs: []
alwaysApply: true
priority: 100
---

# Composer Workflow: Mode Selection Protocol

## Mode Selection Rules

### Use Composer Mode When:
- **Multi-file refactors**: Editing >3 files simultaneously
- **Cross-module changes**: Changes span multiple directories/packages
- **Architecture refactoring**: Moving functions/classes across files
- **Bulk updates**: Find-and-replace across multiple files
- **Feature implementation**: Creating new features that touch multiple files

**Example Triggers**: "Refactor authentication across all modules", "Move utility functions to shared package", "Update all API endpoints"

### Use Chat Mode When:
- **Single-file edits**: Changes limited to one file
- **Code explanations**: Understanding how code works
- **Quick fixes**: Small bug fixes or improvements
- **Code review**: Reviewing specific functions/classes
- **Questions**: Asking about codebase, patterns, or best practices

**Example Triggers**: "Fix the bug in user_service.py", "Explain how this function works", "Add error handling to this method"

### Use Plan Mode When:
- **Architecture design**: Designing new systems or components
- **Complex features**: Multi-step features requiring planning
- **System design**: High-level system architecture decisions
- **Migration planning**: Planning migrations or major refactors
- **Research tasks**: Deep technical research requiring structured output

**Example Triggers**: "Design a new authentication system", "Plan migration from FastAPI to Rust", "Research best practices for Kafka consumer groups"

## Decision Flow

```
User Request
    ↓
Is it architecture/system design?
    ├─ YES → Plan Mode
    └─ NO ↓
Is it multi-file (>3 files)?
    ├─ YES → Composer Mode
    └─ NO ↓
Is it single file or question?
    ├─ YES → Chat Mode
    └─ NO → Chat Mode (default)
```

## Efficiency Guidelines

### Composer Mode Best Practices:
- **Batch related changes**: Group related edits together
- **Use file patterns**: Leverage glob patterns for bulk operations
- **Preview before apply**: Review changes before accepting
- **Atomic commits**: Each Composer session = one logical change

### Chat Mode Best Practices:
- **Be specific**: Reference exact files/functions
- **Use @mentions**: Reference specific files with @filename
- **Ask follow-ups**: Don't hesitate to ask for clarification

### Plan Mode Best Practices:
- **Review plan first**: Always review plan before implementation
- **Break into phases**: Large plans should be split into phases
- **Mark todos**: Use todo list to track implementation progress

## Anti-Patterns

❌ **Don't use Composer for**: Single-line fixes, questions, explanations
❌ **Don't use Chat for**: Multi-file refactors, architecture design
❌ **Don't use Plan for**: Quick fixes, simple questions, single-file edits

## Integration with MCP Tools

**Composer Mode + MCP**:
- Use GitLab MCP to create MRs after Composer sessions
- Use database MCPs to validate changes affect data correctly

**Chat Mode + MCP**:
- Use MCP tools to query systems while discussing code
- Use database MCPs to inspect data during debugging

**Plan Mode + MCP**:
- Use MCP tools to gather system state for planning
- Use GitLab MCP to check existing MRs/issues before planning
