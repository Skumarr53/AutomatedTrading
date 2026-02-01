# src/utils/hardware_detector.py
"""
Hardware detection and optimization utilities for ML training.

Auto-detects CPU cores, RAM, and GPU availability to optimize
parallel processing, hyperparameter search, and model configuration.
"""

import os
import platform
from typing import Dict, Optional
from loguru import logger

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False
    logger.warning("psutil not installed. Install with: uv add psutil")

try:
    import GPUtil
    _HAS_GPUTIL = True
except ImportError:
    _HAS_GPUTIL = False

try:
    import torch
    _HAS_TORCH = True
except ImportError:
    _HAS_TORCH = False


class HardwareDetector:
    """
    Detects and provides optimal hardware configuration for ML training.
    """
    
    def __init__(self):
        self._cache: Optional[Dict] = None
    
    def detect(self) -> Dict:
        """
        Detect hardware specifications.
        
        Returns:
            Dictionary with keys:
            - cpu_cores_physical: Number of physical CPU cores
            - cpu_cores_logical: Number of logical CPU cores (with hyperthreading)
            - ram_gb: Available RAM in GB
            - ram_total_gb: Total RAM in GB
            - has_gpu: Boolean indicating GPU availability
            - gpu_name: GPU name if available
            - optimal_n_jobs: Recommended number of parallel jobs
            - hardware_tier: 'low', 'medium', or 'high'
        """
        if self._cache is not None:
            return self._cache
        
        result = {
            'cpu_cores_physical': self._detect_cpu_physical(),
            'cpu_cores_logical': self._detect_cpu_logical(),
            'ram_gb': self._detect_ram_available(),
            'ram_total_gb': self._detect_ram_total(),
            'has_gpu': False,
            'gpu_name': None,
            'optimal_n_jobs': None,  # Will be set based on mode
            'hardware_tier': None,  # Will be set based on specs
        }
        
        # Detect GPU
        gpu_info = self._detect_gpu()
        result.update(gpu_info)
        
        # Determine hardware tier
        result['hardware_tier'] = self._determine_tier(result)
        
        self._cache = result
        return result
    
    def _detect_cpu_physical(self) -> int:
        """Detect number of physical CPU cores."""
        if _HAS_PSUTIL:
            return psutil.cpu_count(logical=False) or os.cpu_count() or 4
        return os.cpu_count() or 4
    
    def _detect_cpu_logical(self) -> int:
        """Detect number of logical CPU cores (with hyperthreading)."""
        if _HAS_PSUTIL:
            return psutil.cpu_count(logical=True) or os.cpu_count() or 4
        return os.cpu_count() or 4
    
    def _detect_ram_available(self) -> float:
        """Detect available RAM in GB."""
        if _HAS_PSUTIL:
            ram_bytes = psutil.virtual_memory().available
            return round(ram_bytes / (1024 ** 3), 2)
        # Fallback: try to read from /proc/meminfo on Linux
        if platform.system() == 'Linux':
            try:
                with open('/proc/meminfo', 'r') as f:
                    for line in f:
                        if 'MemAvailable' in line:
                            kb = int(line.split()[1])
                            return round(kb / (1024 ** 2), 2)
            except Exception:
                pass
        return 16.0  # Default fallback
    
    def _detect_ram_total(self) -> float:
        """Detect total RAM in GB."""
        if _HAS_PSUTIL:
            ram_bytes = psutil.virtual_memory().total
            return round(ram_bytes / (1024 ** 3), 2)
        # Fallback: try to read from /proc/meminfo on Linux
        if platform.system() == 'Linux':
            try:
                with open('/proc/meminfo', 'r') as f:
                    for line in f:
                        if 'MemTotal' in line:
                            kb = int(line.split()[1])
                            return round(kb / (1024 ** 2), 2)
            except Exception:
                pass
        return 16.0  # Default fallback
    
    def _detect_gpu(self) -> Dict:
        """Detect GPU availability and details."""
        result = {'has_gpu': False, 'gpu_name': None}
        
        # Try PyTorch first (most reliable)
        if _HAS_TORCH and torch.cuda.is_available():
            result['has_gpu'] = True
            result['gpu_name'] = torch.cuda.get_device_name(0)
            result['gpu_memory_gb'] = round(torch.cuda.get_device_properties(0).total_memory / (1024 ** 3), 2)
            return result
        
        # Try GPUtil
        if _HAS_GPUTIL:
            try:
                gpus = GPUtil.getGPUs()
                if gpus:
                    result['has_gpu'] = True
                    result['gpu_name'] = gpus[0].name
                    result['gpu_memory_gb'] = round(gpus[0].memoryTotal / 1024, 2)
                    return result
            except Exception:
                pass
        
        # Try nvidia-smi command
        try:
            import subprocess
            output = subprocess.check_output(['nvidia-smi', '--query-gpu=name,memory.total', '--format=csv,noheader'], 
                                           stderr=subprocess.DEVNULL, timeout=2)
            if output:
                lines = output.decode().strip().split('\n')
                if lines:
                    parts = lines[0].split(',')
                    result['has_gpu'] = True
                    result['gpu_name'] = parts[0].strip()
                    # Parse memory (format: "XXXX MiB")
                    if len(parts) > 1:
                        mem_str = parts[1].strip().replace('MiB', '').strip()
                        result['gpu_memory_gb'] = round(int(mem_str) / 1024, 2)
                    return result
        except Exception:
            pass
        
        return result
    
    def _determine_tier(self, specs: Dict) -> str:
        """
        Determine hardware tier based on specifications.
        
        Tiers:
        - low: <8 cores, <16GB RAM
        - medium: 8-16 cores, 16-32GB RAM
        - high: >16 cores, >32GB RAM
        """
        cores = specs['cpu_cores_logical']
        ram = specs['ram_total_gb']
        
        if cores >= 16 and ram >= 32:
            return 'low'
        elif cores >= 8 and ram >= 16:
            return 'low'
        else:
            return 'low'
    
    def get_optimal_n_jobs(self, mode: str = 'balanced', reserve_cores: int = 2) -> int:
        """
        Get optimal number of parallel jobs based on hardware and mode.
        
        Args:
            mode: 'speed', 'balanced', or 'quality'
            reserve_cores: Number of cores to reserve for system (default: 2)
        
        Returns:
            Optimal number of parallel jobs
        """
        specs = self.detect()
        logical_cores = specs['cpu_cores_logical']
        
        if mode == 'speed':
            # Aggressive: use all cores except 1
            return max(1, logical_cores - 1)
        elif mode == 'balanced':
            # Balanced: leave 2 cores free for system
            return max(1, logical_cores - reserve_cores)
        elif mode == 'quality':
            # Conservative: use half the cores to avoid contention
            return max(1, logical_cores // 2)
        else:
            logger.warning(f"Unknown mode '{mode}', using balanced")
            return max(1, logical_cores - reserve_cores)
    
    def get_optimal_n_iter(self, mode: str = 'balanced') -> int:
        """
        Get optimal number of hyperparameter search iterations.
        
        Args:
            mode: 'speed', 'balanced', or 'quality'
        
        Returns:
            Optimal number of iterations
        """
        tier = self.detect()['hardware_tier']
        
        if mode == 'speed':
            return {'low': 5, 'medium': 10, 'high': 15}[tier]
        elif mode == 'balanced':
            return {'low': 10, 'medium': 15, 'high': 20}[tier]
        else:  # quality
            return {'low': 15, 'medium': 20, 'high': 30}[tier]
    
    def get_optimal_n_splits(self, data_size: int, mode: str = 'balanced') -> int:
        """
        Get optimal number of CV splits based on data size.
        
        Args:
            data_size: Number of samples in dataset
            mode: 'speed', 'balanced', or 'quality'
        
        Returns:
            Optimal number of CV splits
        """
        if mode == 'speed':
            if data_size < 10000:
                return 3
            elif data_size < 50000:
                return 3
            else:
                return 3
        elif mode == 'balanced':
            if data_size < 10000:
                return 3
            elif data_size < 100000:
                return 5
            else:
                return 3  # Fewer splits for large datasets
        else:  # quality
            if data_size < 10000:
                return 5
            elif data_size < 100000:
                return 5
            else:
                return 5
    
    def get_max_samples_per_class(self, mode: str = 'balanced') -> int:
        """
        Get optimal max_samples_per_class based on RAM.
        
        Args:
            mode: 'speed', 'balanced', or 'quality'
        
        Returns:
            Maximum samples per class
        """
        ram_gb = self.detect()['ram_total_gb']
        
        if mode == 'speed':
            # Lower limits for faster processing
            if ram_gb < 16:
                return 2000
            elif ram_gb < 32:
                return 3000
            else:
                return 4000
        elif mode == 'balanced':
            if ram_gb < 16:
                return 3000
            elif ram_gb < 32:
                return 5000
            else:
                return 8000
        else:  # quality
            if ram_gb < 16:
                return 5000
            elif ram_gb < 32:
                return 8000
            else:
                return 12000
    
    def log_hardware_info(self):
        """Log detected hardware information."""
        specs = self.detect()
        logger.info("=" * 60)
        logger.info("Hardware Detection Results:")
        logger.info(f"  CPU Cores (Physical/Logical): {specs['cpu_cores_physical']}/{specs['cpu_cores_logical']}")
        logger.info(f"  RAM (Available/Total): {specs['ram_gb']:.1f}GB / {specs['ram_total_gb']:.1f}GB")
        logger.info(f"  GPU: {'Yes' if specs['has_gpu'] else 'No'}")
        if specs['has_gpu']:
            logger.info(f"    Name: {specs.get('gpu_name', 'Unknown')}")
            if 'gpu_memory_gb' in specs:
                logger.info(f"    Memory: {specs['gpu_memory_gb']:.1f}GB")
        logger.info(f"  Hardware Tier: {specs['hardware_tier'].upper()}")
        logger.info("=" * 60)


# Global singleton instance
_hardware_detector = HardwareDetector()


def get_hardware_detector() -> HardwareDetector:
    """Get the global hardware detector instance."""
    return _hardware_detector
