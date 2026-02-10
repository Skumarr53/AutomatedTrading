---
description: SYSTEM CONTEXT - Arch/CachyOS, Fish Shell, Podman, and Tooling.
globs: ["**/*"]
alwaysApply: true
---

# 🖥️ System Context: The Arch/AIOps Forge

## 1. OS & Shell Constraints
- **OS**: CachyOS (Arch-based, Rolling). **Assume latest packages**.
- **Shell**: `fish`. **Non-Negotiable**.
- **Syntax Enforcement**:
    - ❌ `export VAR=x` | `source venv/bin/activate` | `&&` | `$(cmd)`
    - ✅ `set -x VAR x` | `source .venv/bin/activate.fish` | `; and` | `(cmd)`

## 2. Shell Refusal Protocol (MANDATORY)
**STOP & REFUSE** if any Bash-style syntax is used in commands or scripts:
- ❌ **Use of `export`**: Must use `set -x`.
- ❌ **Use of `&&`**: Must use `; and`.
- ❌ **Use of `$()`**: Must use `(cmd)`.
- ❌ **Use of `source venv/bin/activate`**: Must use `.fish` variant.

**Enforcement Action**: If you detect yourself generating Bash syntax, **REDO** the command immediately in Fish syntax.

## 3. Containerization (Podman)
- **Runtime**: Podman (Rootless). **Non-Negotiable**.
- **Network**: `podman-compose`.
- **Storage**: BTRFS compatible drivers (`overlay`).
- **Data Persistence**: All project-related data must persist within the project folder itself.

## 4. Containerization Refusal Protocol (MANDATORY)
**STOP & REFUSE** if `docker` or `docker-compose` is used:
- ❌ `docker run ...` | `docker build ...`
- ✅ `podman run ...` | `podman build ...`
- ❌ `docker-compose up`
- ✅ `podman-compose up`

**Enforcement Action**: Always use `/var/lib/docker` as the primary reference container service. If you detect yourself generating `docker` commands, **REDO** them immediately using `podman`. Never initialize a new container/image if one already exists for the task.

## 5. Version Control (GitLab)
- **Default Provider**: GitLab. **Non-Negotiable**.
- **CLI Tooling**: `glab` (GitLab CLI).
- **Workflows**: Use `glab mr` for merge requests, `glab pipeline` for CI/CD, and `glab issue` for tracking.

## 6. Version Control Refusal Protocol (MANDATORY)
**STOP & REFUSE** if GitHub-specific commands or `gh` CLI are used:
- ❌ `gh pr create` | `gh issue list`
- ✅ `glab mr create` | `glab issue list`
- ❌ Referring to "Pull Requests" (PRs)
- ✅ Use "Merge Requests" (MRs)

**Enforcement Action**: If you detect yourself generating GitHub/`gh` commands, **REDO** them immediately using GitLab/`glab`.

## 7. Python <-> Rust Bridge
- **Tooling**: `uv` is the only project manager allowed.
- **Linting**: `ruff` (strict mode).
- **Testing**: `pytest` with `pytest-asyncio`.

## 8. Scripting Standards
- **Shebang**: `#!/usr/bin/env fish`. **No Bash fallback allowed**.
- **chmod**: ALL generated scripts must include `chmod +x script_name` in the generation block.
- **Location**:
    - Project: `./scripts/`
    - Global: `~/.scripts/`

## 9. Arch Linux Mindset
- Latest is greatest (but know how to downgrade)
- Breakage is learning opportunity
- Source code available for everything
- DIY > waiting for package
- Community knowledge > official documentation
- System transparency - no black boxes

## 10. Dev/Local Hardware 

### CPU
| Specification | Value |
|--------------|--------|
| **Processor** | AMD Ryzen 9 9900X |
| **Cores** | 12 |
| **Threads** | 24 (2 threads per core) |
| **Architecture** | x86_64 |
| **Base Frequency** | 614 MHz |
| **Boost Frequency** | 5,662 MHz |
| **Virtualization** | AMD-V ✅ |

### GPU
| Specification | Value |
|--------------|--------|
| **GPU** | AMD Radeon AI PRO R9700 (Navi 48) |
| **Type** | Integrated/Discrete |

### Memory (RAM)
| Metric | Value |
|--------|--------|
| **Total RAM** | 62 GiB |