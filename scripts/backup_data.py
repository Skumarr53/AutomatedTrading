#!/usr/bin/env python3
"""
scripts/backup_data.py
Automated backup system for InfluxDB, MLflow, and configuration files.

Backs up:
- InfluxDB databases (full and incremental)
- MLflow database and artifacts
- Configuration files (YAML)
- Model registry

Usage:
    python scripts/backup_data.py [--full] [--incremental] [--cleanup]
"""
from __future__ import annotations

import argparse
import gzip
import os
import shutil
import subprocess
import sys
import tarfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from loguru import logger

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

BACKUP_ROOT = PROJECT_ROOT / "data" / "backups"
BACKUP_RETENTION_DAYS = 30  # Keep backups for 30 days


def ensure_backup_directory() -> Path:
    """Ensure backup directory exists."""
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    return BACKUP_ROOT


def get_backup_timestamp() -> str:
    """Get timestamp string for backup directory."""
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def backup_influxdb(dest_dir: Path, full: bool = True) -> bool:
    """
    Backup InfluxDB database.
    
    Args:
        dest_dir: Destination directory for backup
        full: If True, full backup; if False, incremental
        
    Returns:
        True if backup successful, False otherwise
    """
    try:
        # Check if InfluxDB container is running
        result = subprocess.run(
            ["podman", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if "trading-influxdb" not in result.stdout:
            logger.warning("InfluxDB container not running, skipping backup")
            return False
        
        logger.info(f"Backing up InfluxDB ({'full' if full else 'incremental'})...")
        
        # Create backup directory
        influx_backup_dir = dest_dir / "influxdb"
        influx_backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Use influx backup command (requires InfluxDB CLI in container)
        # Alternative: backup the data directory directly
        influx_data_dir = PROJECT_ROOT / "data" / "influxdb"
        
        if influx_data_dir.exists():
            # Create tar.gz archive of InfluxDB data
            backup_file = influx_backup_dir / f"influxdb_{'full' if full else 'incremental'}.tar.gz"
            
            with tarfile.open(backup_file, "w:gz") as tar:
                tar.add(influx_data_dir, arcname="influxdb", recursive=True)
            
            backup_size = backup_file.stat().st_size / (1024 * 1024)  # MB
            logger.success(f"InfluxDB backup created: {backup_file} ({backup_size:.2f} MB)")
            return True
        else:
            logger.warning(f"InfluxDB data directory not found: {influx_data_dir}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to backup InfluxDB: {e}")
        return False


def backup_mlflow(dest_dir: Path) -> bool:
    """
    Backup MLflow database and artifacts.
    
    Args:
        dest_dir: Destination directory for backup
        
    Returns:
        True if backup successful, False otherwise
    """
    try:
        logger.info("Backing up MLflow...")
        
        mlflow_backup_dir = dest_dir / "mlflow"
        mlflow_backup_dir.mkdir(parents=True, exist_ok=True)
        
        mlflow_data_dir = PROJECT_ROOT / "data" / "mlflow"
        
        if not mlflow_data_dir.exists():
            logger.warning(f"MLflow data directory not found: {mlflow_data_dir}")
            return False
        
        # Backup MLflow database and artifacts
        backup_file = mlflow_backup_dir / "mlflow_backup.tar.gz"
        
        with tarfile.open(backup_file, "w:gz") as tar:
            tar.add(mlflow_data_dir, arcname="mlflow", recursive=True)
        
        backup_size = backup_file.stat().st_size / (1024 * 1024)  # MB
        logger.success(f"MLflow backup created: {backup_file} ({backup_size:.2f} MB)")
        return True
        
    except Exception as e:
        logger.error(f"Failed to backup MLflow: {e}")
        return False


def backup_configs(dest_dir: Path) -> bool:
    """
    Backup configuration files.
    
    Args:
        dest_dir: Destination directory for backup
        
    Returns:
        True if backup successful, False otherwise
    """
    try:
        logger.info("Backing up configuration files...")
        
        config_backup_dir = dest_dir / "configs"
        config_backup_dir.mkdir(parents=True, exist_ok=True)
        
        config_dir = PROJECT_ROOT / "src" / "config"
        
        if not config_dir.exists():
            logger.warning(f"Config directory not found: {config_dir}")
            return False
        
        # Copy all YAML files
        config_files = list(config_dir.rglob("*.yaml")) + list(config_dir.rglob("*.yml"))
        
        if not config_files:
            logger.warning("No config files found")
            return False
        
        # Create tar.gz archive
        backup_file = config_backup_dir / "configs_backup.tar.gz"
        
        with tarfile.open(backup_file, "w:gz") as tar:
            for config_file in config_files:
                arcname = config_file.relative_to(PROJECT_ROOT)
                tar.add(config_file, arcname=str(arcname))
        
        backup_size = backup_file.stat().st_size / (1024 * 1024)  # MB
        logger.success(f"Config backup created: {backup_file} ({len(config_files)} files, {backup_size:.2f} MB)")
        return True
        
    except Exception as e:
        logger.error(f"Failed to backup configs: {e}")
        return False


def cleanup_old_backups(keep_days: int = BACKUP_RETENTION_DAYS) -> int:
    """
    Remove backups older than specified days.
    
    Args:
        keep_days: Number of days to keep backups
        
    Returns:
        Number of backups removed
    """
    try:
        if not BACKUP_ROOT.exists():
            return 0
        
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        removed_count = 0
        
        for backup_dir in BACKUP_ROOT.iterdir():
            if not backup_dir.is_dir():
                continue
            
            # Extract date from directory name (format: YYYY-MM-DD_HH-MM-SS)
            try:
                dir_date_str = backup_dir.name.split("_")[0]  # Get YYYY-MM-DD part
                dir_date = datetime.strptime(dir_date_str, "%Y-%m-%d")
                
                if dir_date < cutoff_date:
                    logger.info(f"Removing old backup: {backup_dir.name}")
                    shutil.rmtree(backup_dir)
                    removed_count += 1
            except (ValueError, IndexError):
                # Skip directories that don't match expected format
                continue
        
        if removed_count > 0:
            logger.info(f"Cleaned up {removed_count} old backup(s)")
        
        return removed_count
        
    except Exception as e:
        logger.error(f"Failed to cleanup old backups: {e}")
        return 0


def create_backup(full: bool = True, incremental: bool = False) -> bool:
    """
    Create a backup of all data.
    
    Args:
        full: Create full backup
        incremental: Create incremental backup (if supported)
        
    Returns:
        True if backup successful, False otherwise
    """
    ensure_backup_directory()
    
    timestamp = get_backup_timestamp()
    backup_type = "full" if full else "incremental"
    backup_dir = BACKUP_ROOT / f"{timestamp}_{backup_type}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting {backup_type} backup to: {backup_dir}")
    
    results = []
    
    # Backup InfluxDB
    results.append(backup_influxdb(backup_dir, full=full))
    
    # Backup MLflow
    results.append(backup_mlflow(backup_dir))
    
    # Backup configs
    results.append(backup_configs(backup_dir))
    
    # Create backup manifest
    manifest_file = backup_dir / "backup_manifest.txt"
    with open(manifest_file, "w") as f:
        f.write(f"Backup Type: {backup_type}\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"Created: {datetime.now().isoformat()}\n")
        f.write(f"\nComponents:\n")
        f.write(f"  InfluxDB: {'✓' if results[0] else '✗'}\n")
        f.write(f"  MLflow: {'✓' if results[1] else '✗'}\n")
        f.write(f"  Configs: {'✓' if results[2] else '✗'}\n")
    
    success_count = sum(results)
    total_size = sum(
        f.stat().st_size for f in backup_dir.rglob("*") if f.is_file()
    ) / (1024 * 1024)  # MB
    
    if success_count > 0:
        logger.success(
            f"Backup completed: {success_count}/3 components backed up "
            f"({total_size:.2f} MB total)"
        )
        return True
    else:
        logger.error("Backup failed: No components backed up successfully")
        return False


def main():
    """Main backup function."""
    parser = argparse.ArgumentParser(description="Backup AutomatedTrading data")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Create full backup (default)"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Create incremental backup"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up old backups (older than retention period)"
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=BACKUP_RETENTION_DAYS,
        help=f"Number of days to keep backups (default: {BACKUP_RETENTION_DAYS})"
    )
    
    args = parser.parse_args()
    
    # Cleanup old backups if requested
    if args.cleanup:
        cleanup_old_backups(keep_days=args.retention_days)
    
    # Create backup
    full = not args.incremental  # Default to full if not specified
    success = create_backup(full=full, incremental=args.incremental)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
