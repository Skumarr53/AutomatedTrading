#!/bin/bash
# scripts/schedule_backups.sh
# Schedule automated backups using cron
# 
# Usage:
#   ./schedule_backups.sh install    # Install cron job
#   ./schedule_backups.sh uninstall # Remove cron job
#   ./schedule_backups.sh test       # Run backup manually

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKUP_SCRIPT="$SCRIPT_DIR/backup_data.py"

# Cron schedule
# Daily full backup at 2 AM
DAILY_CRON="0 2 * * *"
# Hourly incremental backup
HOURLY_CRON="0 * * * *"

install_cron() {
    echo "Installing backup cron jobs..."
    
    # Get Python path
    PYTHON_PATH=$(which python3 || which python)
    
    # Create cron entries
    CRON_ENTRIES=(
        "# AutomatedTrading Daily Full Backup (2 AM)"
        "${DAILY_CRON} cd ${PROJECT_ROOT} && ${PYTHON_PATH} ${BACKUP_SCRIPT} --full --cleanup >> ${PROJECT_ROOT}/data/logs/backup.log 2>&1"
        ""
        "# AutomatedTrading Hourly Incremental Backup"
        "${HOURLY_CRON} cd ${PROJECT_ROOT} && ${PYTHON_PATH} ${BACKUP_SCRIPT} --incremental >> ${PROJECT_ROOT}/data/logs/backup.log 2>&1"
    )
    
    # Check if already installed
    if crontab -l 2>/dev/null | grep -q "AutomatedTrading.*Backup"; then
        echo "Backup cron jobs already installed"
        return
    fi
    
    # Add to crontab
    (crontab -l 2>/dev/null; printf '%s\n' "${CRON_ENTRIES[@]}") | crontab -
    
    echo "✓ Backup cron jobs installed"
    echo "  - Daily full backup: 2 AM"
    echo "  - Hourly incremental backup"
    echo ""
    echo "View logs: tail -f ${PROJECT_ROOT}/data/logs/backup.log"
}

uninstall_cron() {
    echo "Removing backup cron jobs..."
    
    crontab -l 2>/dev/null | grep -v "AutomatedTrading.*Backup" | crontab - || true
    
    echo "✓ Backup cron jobs removed"
}

test_backup() {
    echo "Running test backup..."
    cd "$PROJECT_ROOT"
    python3 "$BACKUP_SCRIPT" --full
    echo "✓ Test backup completed"
}

case "${1:-}" in
    install)
        install_cron
        ;;
    uninstall)
        uninstall_cron
        ;;
    test)
        test_backup
        ;;
    *)
        echo "Usage: $0 {install|uninstall|test}"
        exit 1
        ;;
esac
