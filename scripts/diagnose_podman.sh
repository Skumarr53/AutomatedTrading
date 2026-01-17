#!/usr/bin/env bash
# Comprehensive Podman diagnostic - run BEFORE any fixes
# Why: Prevents troubleshooting loops by revealing ALL issues upfront

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_section() { echo -e "\n${BLUE}=== $1 ===${NC}"; }

ISSUES=0
FIXES_NEEDED=()

log_section "SYSTEM STATE"
echo "Kernel version: $(uname -r)"
echo "Uptime: $(uptime -p 2>/dev/null || uptime)"

# Check if kernel modules directory exists
KERNEL_MODULES_DIR="/lib/modules/$(uname -r)"
if [ -d "$KERNEL_MODULES_DIR" ]; then
    log_info "✓ Kernel modules directory exists"
else
    log_error "✗ Kernel modules directory missing: $KERNEL_MODULES_DIR"
    log_warn "  → System likely needs reboot after kernel update"
    ((ISSUES++))
    FIXES_NEEDED+=("REBOOT: Kernel modules missing - run 'sudo reboot'")
fi

log_section "DEPENDENCIES"
MISSING_DEPS=()
for cmd in podman slirp4netns fuse-overlayfs; do
    if command -v "$cmd" &>/dev/null; then
        log_info "✓ $cmd: $(command -v $cmd)"
    else
        log_error "✗ $cmd: MISSING"
        ((ISSUES++))
        case "$cmd" in
            podman)
                FIXES_NEEDED+=("INSTALL: paru -S podman")
                ;;
            slirp4netns)
                FIXES_NEEDED+=("INSTALL: paru -S slirp4netns")
                ;;
            fuse-overlayfs)
                FIXES_NEEDED+=("INSTALL: paru -S fuse-overlayfs")
                ;;
        esac
    fi
done

log_section "KERNEL MODULES"
if sudo modprobe -n tun 2>/dev/null; then
    log_info "✓ tun module: Available"
    if [ -c /dev/net/tun ]; then
        log_info "✓ TUN device: Exists"
    else
        log_warn "✗ TUN device: Missing (module not loaded)"
        ((ISSUES++))
        FIXES_NEEDED+=("LOAD: sudo modprobe tun && echo 'tun' | sudo tee /etc/modules-load.d/tun.conf")
    fi
else
    log_error "✗ tun module: Cannot load (kernel mismatch?)"
    ((ISSUES++))
    FIXES_NEEDED+=("REBOOT: Kernel modules missing - run 'sudo reboot'")
fi

log_section "CONFIGURATION"
STORAGE_CONF="$HOME/.config/containers/storage.conf"
CONTAINERS_CONF="$HOME/.config/containers/containers.conf"

if [ -f "$STORAGE_CONF" ]; then
    log_info "✓ storage.conf exists"
    if grep -q 'driver.*overlay' "$STORAGE_CONF" 2>/dev/null; then
        DRIVER=$(grep -oP 'driver\s*=\s*"\K[^"]+' "$STORAGE_CONF" | head -1)
        log_info "  Driver: $DRIVER"
        
        # Check for mount_program in correct section
        if grep -q '\[storage.options.overlay\]' "$STORAGE_CONF" && grep -q 'mount_program' "$STORAGE_CONF"; then
            log_info "✓ mount_program configured correctly"
        elif grep -q 'mount_program' "$STORAGE_CONF"; then
            log_warn "⚠ mount_program in wrong section (should be [storage.options.overlay])"
            ((ISSUES++))
            FIXES_NEEDED+=("FIX: Update storage.conf to use [storage.options.overlay] section")
        fi
    else
        log_warn "⚠ Driver not set to overlay"
        ((ISSUES++))
        FIXES_NEEDED+=("FIX: Set driver = 'overlay' in storage.conf")
    fi
else
    log_warn "✗ storage.conf: Missing"
    ((ISSUES++))
    FIXES_NEEDED+=("CREATE: Run setup_infrastructure.sh to create config")
fi

if [ -f "$CONTAINERS_CONF" ]; then
    log_info "✓ containers.conf exists"
    if grep -q "slirp4netns" "$CONTAINERS_CONF" 2>/dev/null; then
        log_info "✓ Network backend configured (slirp4netns)"
    else
        log_warn "⚠ Network backend not configured"
        ((ISSUES++))
        FIXES_NEEDED+=("FIX: Configure network backend in containers.conf")
    fi
else
    log_warn "✗ containers.conf: Missing"
    ((ISSUES++))
    FIXES_NEEDED+=("CREATE: Run setup_infrastructure.sh to create config")
fi

log_section "STORAGE DATABASE STATE"
STORAGE_DB="$HOME/.local/share/containers/storage"
if [ -d "$STORAGE_DB" ]; then
    log_info "✓ Storage database exists"
    
    # Try to get storage driver from database
    if podman info &>/dev/null; then
        DB_DRIVER=$(podman info --format "{{.Store.GraphDriverName}}" 2>&1 || echo "unknown")
        CONFIG_DRIVER=$(grep -oP 'driver\s*=\s*"\K[^"]+' "$STORAGE_CONF" 2>/dev/null | head -1 || echo "unknown")
        
        if [ "$DB_DRIVER" != "$CONFIG_DRIVER" ] && [ "$DB_DRIVER" != "unknown" ]; then
            log_error "✗ Storage driver mismatch!"
            log_error "  Database: $DB_DRIVER"
            log_error "  Config: $CONFIG_DRIVER"
            ((ISSUES++))
            FIXES_NEEDED+=("RESET: rm -rf ~/.local/share/containers/storage (then hide config temporarily)")
        else
            log_info "✓ Storage driver matches config"
        fi
    else
        log_warn "⚠ Cannot query Podman info (may indicate config conflict)"
        ((ISSUES++))
    fi
else
    log_info "✓ Storage database doesn't exist (will be created fresh)"
fi

log_section "RUNTIME STATE"
if podman info &>/dev/null 2>&1; then
    log_info "✓ Podman can start"
    PODMAN_VERSION=$(podman --version 2>/dev/null || echo "unknown")
    log_info "  Version: $PODMAN_VERSION"
    
    # Try a simple command
    if podman ps &>/dev/null; then
        log_info "✓ Podman commands work"
    else
        log_error "✗ Podman commands fail"
        ((ISSUES++))
    fi
else
    log_error "✗ Podman cannot start"
    log_error "  Error: $(podman info 2>&1 | head -3)"
    ((ISSUES++))
fi

log_section "SUMMARY"
if [ $ISSUES -eq 0 ]; then
    log_info "✓ All checks passed! Podman should work correctly."
    exit 0
else
    log_error "Found $ISSUES issue(s) that need fixing:"
    echo ""
    for i in "${!FIXES_NEEDED[@]}"; do
        echo "  $((i+1)). ${FIXES_NEEDED[$i]}"
    done
    echo ""
    log_warn "Run fixes in order, then re-run this diagnostic."
    exit 1
fi
