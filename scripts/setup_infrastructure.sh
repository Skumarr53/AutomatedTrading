#!/usr/bin/env bash
# setup_infrastructure.sh
# Bootstrap script for setting up the trading infrastructure
#
# Usage: ./scripts/setup_infrastructure.sh [--dev|--prod]
#
# Requirements:
#   - Podman installed (paru -S podman podman-compose)
#   - Python 3.10+ with uv

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_MODE="${1:-dev}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check and fix Podman storage configuration for BTRFS
check_podman_storage() {
    local storage_conf="$HOME/.config/containers/storage.conf"
    
    # Check if storage config exists and is correct
    if [ ! -f "$storage_conf" ] || ! grep -q 'driver = "overlay"' "$storage_conf" 2>/dev/null; then
        log_warn "Configuring Podman storage for BTRFS filesystem..."
        log_info "Why: BTRFS doesn't support kernel overlayfs - need overlay driver with fuse-overlayfs"
        mkdir -p "$(dirname "$storage_conf")"
        
        # Check if fuse-overlayfs is available (required for BTRFS)
        if command -v fuse-overlayfs &> /dev/null; then
            log_info "Using overlay driver with fuse-overlayfs (userspace overlay - BTRFS compatible)"
            cat > "$storage_conf" << 'STORAGECONF'
[storage]
driver = "overlay"

[storage.options.overlay]
mount_program = "/usr/bin/fuse-overlayfs"
STORAGECONF
        else
            log_warn "fuse-overlayfs not found. Installing..."
            log_info "Why: Required for overlay driver on BTRFS filesystem"
            if command -v paru &> /dev/null; then
                paru -S --noconfirm fuse-overlayfs || {
                    log_error "Failed to install fuse-overlayfs"
                    log_info "Fallback: Using vfs driver (slower but compatible)"
                    cat > "$storage_conf" << 'STORAGECONF'
[storage]
driver = "vfs"

[storage.options]
STORAGECONF
                    log_info "Fix: Using vfs driver (copies files directly, no mount_program needed)"
                    return
                }
                # Retry with fuse-overlayfs after install
                cat > "$storage_conf" << 'STORAGECONF'
[storage]
driver = "overlay"

[storage.options.overlay]
mount_program = "/usr/bin/fuse-overlayfs"
STORAGECONF
            else
                log_error "paru not found. Install fuse-overlayfs manually: paru -S fuse-overlayfs"
                exit 1
            fi
        fi
        
        log_info "✓ Podman storage configured"
        log_info "Fix: Using overlay driver with fuse-overlayfs for BTRFS compatibility"
    fi
}

# Check and fix Podman network configuration
check_podman_network() {
    local containers_conf="$HOME/.config/containers/containers.conf"
    
    # Check if network config exists
    if [ ! -f "$containers_conf" ] || ! grep -q "slirp4netns" "$containers_conf" 2>/dev/null; then
        log_warn "Configuring Podman network backend..."
        log_info "Why: pasta backend requires /dev/net/tun which may not be available"
        mkdir -p "$(dirname "$containers_conf")"
        
        # Use slirp4netns as fallback (more compatible)
        cat > "$containers_conf" << 'NETCONF'
[containers]
[engine]
[network]
network_backend = "netavark"
default_rootless_network_cmd = "slirp4netns"
NETCONF
        
        log_info "✓ Podman network configured"
        log_info "Fix: Using slirp4netns instead of pasta for better compatibility"
    fi
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Podman
    if ! command -v podman &> /dev/null; then
        log_error "Podman is not installed. Install with: paru -S podman"
        exit 1
    fi
    log_info "✓ Podman found: $(podman --version)"

    # Check slirp4netns (required for our network config)
    if ! command -v slirp4netns &> /dev/null; then
        log_warn "slirp4netns not found. Installing..."
        if command -v paru &> /dev/null; then
            paru -S --noconfirm slirp4netns || log_error "Failed to install slirp4netns"
        else
            log_error "Please install slirp4netns manually (paru -S slirp4netns)"
        fi
    fi
    log_info "✓ slirp4netns found"

    # Check for TUN device (required for slirp4netns/pasta)
    if [ ! -c /dev/net/tun ]; then
        log_warn "TUN device missing (/dev/net/tun)"
        log_info "Why: Required for rootless networking"
        log_info "Fixing: Attempting to load 'tun' module..."
        sudo modprobe tun || {
            log_error "Failed to load 'tun' kernel module"
            log_info "Try: sudo modprobe tun"
            exit 1
        }
        echo "tun" | sudo tee /etc/modules-load.d/tun.conf > /dev/null
        log_info "✓ TUN module loaded and configured for auto-load"
    fi
    log_info "✓ TUN device ready"
    
    # Check and configure Podman storage
    check_podman_storage
    
    # Check and configure Podman network
    check_podman_network
    
    # Check podman-compose
    if ! command -v podman-compose &> /dev/null; then
        log_warn "podman-compose not found. Installing..."
        pip install podman-compose || {
            log_error "Failed to install podman-compose"
            exit 1
        }
    fi
    log_info "✓ podman-compose found"
    
    # Check Python
    if ! command -v python &> /dev/null; then
        log_error "Python is not installed"
        exit 1
    fi
    log_info "✓ Python found: $(python --version)"
    
    # Check uv
    if ! command -v uv &> /dev/null; then
        log_warn "uv not found. Consider installing for better dependency management"
    else
        log_info "✓ uv found: $(uv --version)"
    fi
}

# Setup environment file
setup_env_file() {
    log_info "Setting up environment file..."
    
    if [[ ! -f "$PROJECT_ROOT/.env" ]]; then
        if [[ -f "$PROJECT_ROOT/.env.example" ]]; then
            cp "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"
            log_warn "Created .env from .env.example - please update with your credentials"
        else
            log_error ".env.example not found"
            exit 1
        fi
    else
        log_info "✓ .env file already exists"
    fi
}

# Create Podman network if it doesn't exist
create_network() {
    if ! podman network exists trading-network; then
        log_info "Creating Podman network: trading-network"
        podman network create trading-network || log_warn "Failed to create network, it may already exist"
    else
        log_info "✓ Podman network 'trading-network' already exists"
    fi
}

# Start InfluxDB container
start_influxdb() {
    log_info "Starting InfluxDB container..."
    
    cd "$PROJECT_ROOT"
    
    # Ensure network exists
    create_network
    
    # Check if container already running
    if podman ps --format "{{.Names}}" | grep -q "trading-influxdb"; then
        log_info "✓ InfluxDB container already running"
        return 0
    fi
    
    # Start with podman-compose
    podman-compose up -d influxdb
    
    # Wait for InfluxDB to be ready
    log_info "Waiting for InfluxDB to be ready..."
    local max_attempts=30
    local attempt=0
    
    while [[ $attempt -lt $max_attempts ]]; do
        if podman exec trading-influxdb influx ping &> /dev/null; then
            log_info "✓ InfluxDB is ready"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    
    log_error "InfluxDB failed to start within timeout"
    exit 1
}

# Setup InfluxDB buckets
setup_influxdb_buckets() {
    log_info "Setting up InfluxDB buckets..."
    
    # Load environment variables
    source "$PROJECT_ROOT/.env"
    
    local buckets=("ticker_data" "order_book" "trades" "system_metrics")
    
    for bucket in "${buckets[@]}"; do
        log_info "Creating bucket: $bucket"
        podman exec trading-influxdb influx bucket create \
            --name "$bucket" \
            --org "${INFLUXDB_ORG:-trading}" \
            --token "${INFLUXDB_TOKEN:-trading_influxdb_token_change_in_prod}" \
            --retention 365d 2>/dev/null || log_info "Bucket $bucket may already exist"
    done
    
    log_info "✓ InfluxDB buckets configured"
}

# Start MLflow
start_mlflow() {
    log_info "Starting MLflow container..."
    
    cd "$PROJECT_ROOT"
    
    # Ensure network exists
    create_network
    
    # PREVENT permission issues by ensuring permissions are correct BEFORE starting
    log_info "Preventing MLflow permission issues..."
    if [ -f "${PROJECT_ROOT}/scripts/prevent_mlflow_permission_issues.sh" ]; then
        bash "${PROJECT_ROOT}/scripts/prevent_mlflow_permission_issues.sh" || {
            log_warn "Permission prevention script had issues, continuing anyway..."
        }
    fi
    
    # Fix permissions for rootless Podman (if directories exist)
    if [ -d "${PROJECT_ROOT}/data/mlflow" ]; then
        log_info "Fixing MLflow directory permissions for rootless Podman..."
        # Use the dedicated permission fix script for consistency
        if [ -f "${PROJECT_ROOT}/scripts/ensure_mlflow_permissions.sh" ]; then
            bash "${PROJECT_ROOT}/scripts/ensure_mlflow_permissions.sh" || {
                log_warn "Permission fix script had issues, trying direct fix..."
                podman unshare chmod -R 777 "${PROJECT_ROOT}/data/mlflow" 2>/dev/null || true
                podman unshare chmod -R 777 "${PROJECT_ROOT}/data/mlartifacts" 2>/dev/null || true
            }
        else
            # Fallback to direct fix
            podman unshare chmod -R 777 "${PROJECT_ROOT}/data/mlflow" 2>/dev/null || true
            podman unshare chmod -R 777 "${PROJECT_ROOT}/data/mlartifacts" 2>/dev/null || true
        fi
    else
        # Create directories with correct permissions from the start
        mkdir -p "${PROJECT_ROOT}/data/mlflow"
        mkdir -p "${PROJECT_ROOT}/data/mlartifacts"
        podman unshare chmod -R 777 "${PROJECT_ROOT}/data/mlflow" 2>/dev/null || true
        podman unshare chmod -R 777 "${PROJECT_ROOT}/data/mlartifacts" 2>/dev/null || true
    fi
    
    if podman ps --format "{{.Names}}" | grep -q "trading-mlflow"; then
        log_info "✓ MLflow container already running"
        return 0
    fi
    
    # Try to start with podman-compose (will pull or build locally)
    if ! podman-compose up -d mlflow; then
        log_error "Failed to start MLflow container via compose"
        log_info "Trying manual local build fallback..."
        _build_python_mlflow || return 1
    fi
    
    # Wait for MLflow
    local max_attempts=20
    local attempt=0
    
    while [[ $attempt -lt $max_attempts ]]; do
        if curl -s http://localhost:5000/health &> /dev/null; then
            log_info "✓ MLflow is ready"
            
            # Verify container can write to artifacts directory
            if podman exec trading-mlflow sh -c "test -w /mlflow/artifacts" 2>/dev/null; then
                log_info "✓ Container write permissions verified"
            else
                log_warn "Container cannot write to /mlflow/artifacts - fixing permissions..."
                podman exec trading-mlflow sh -c "chmod -R 777 /mlflow /mlflow/artifacts" 2>/dev/null || {
                    log_warn "Could not fix permissions from inside container"
                    log_info "Run: bash scripts/ensure_mlflow_permissions.sh"
                }
            fi
            
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    
    log_warn "MLflow may still be starting (check logs: podman logs trading-mlflow)"
}

# Build Python-based MLflow server (fallback)
_build_python_mlflow() {
    log_info "Building Python-based MLflow server..."
    
    # Ensure network exists (just in case)
    create_network
    
    if [ ! -f "$PROJECT_ROOT/Dockerfile.mlflow" ]; then
        log_error "Dockerfile.mlflow not found"
        return 1
    fi
    
    if podman build -f "$PROJECT_ROOT/Dockerfile.mlflow" -t trading-mlflow:local; then
        log_info "✓ Python MLflow image built successfully"
        
        # Update compose.yml temporarily to use local image
        # Or start container directly
        podman run -d \
            --name trading-mlflow \
            --network trading-network \
            -p 5000:5000 \
            -v mlflow-data:/mlflow \
            -e MLFLOW_TRACKING_URI=http://0.0.0.0:5000 \
            trading-mlflow:local || {
            log_error "Failed to start Python MLflow container"
            return 1
        }
        
        log_info "✓ Python-based MLflow container started"
        return 0
    else
        log_error "Failed to build Python MLflow image"
        return 1
    fi
}

# Install Python dependencies
install_dependencies() {
    log_info "Installing Python dependencies..."
    
    cd "$PROJECT_ROOT"
    
    if command -v uv &> /dev/null; then
        uv pip install -e ".[dev]"
    else
        pip install -e ".[dev]"
    fi
    
    log_info "✓ Python dependencies installed"
}

# Create required directories
setup_directories() {
    log_info "Creating required directories..."
    
    local dirs=(
        "$PROJECT_ROOT/backups/TickerData"
        "$PROJECT_ROOT/backups/OrderBookData"
        "$PROJECT_ROOT/model_artifacts"
        "$PROJECT_ROOT/logs"
        "$PROJECT_ROOT/data/cache"
    )
    
    for dir in "${dirs[@]}"; do
        mkdir -p "$dir"
    done
    
    log_info "✓ Directories created"
}

# Run health checks
run_health_checks() {
    log_info "Running health checks..."
    
    # Check InfluxDB
    if curl -s http://localhost:8086/health | grep -q "pass"; then
        log_info "✓ InfluxDB health check passed"
    else
        log_warn "InfluxDB health check failed"
    fi
    
    # Check MLflow
    if curl -s http://localhost:5000/health &> /dev/null; then
        log_info "✓ MLflow health check passed"
    else
        log_warn "MLflow health check failed or not started"
    fi
}

# Print summary
print_summary() {
    echo ""
    echo "=================================================="
    echo "              Setup Complete!"
    echo "=================================================="
    echo ""
    echo "Services:"
    echo "  - InfluxDB:  http://localhost:8086"
    echo "  - MLflow:    http://localhost:5000"
    echo ""
    echo "Next steps:"
    echo "  1. Update .env with your Fyers API credentials"
    echo "  2. Run migration: python scripts/migrate_data2Influx.py"
    echo "  3. Start trading: python main.py"
    echo ""
    echo "Useful commands:"
    echo "  - View containers:   podman ps"
    echo "  - View logs:         podman-compose logs -f"
    echo "  - Stop all:          podman-compose down"
    echo "  - Run tests:         pytest tests/ -v"
    echo ""
}

# Main execution
main() {
    log_info "Starting infrastructure setup (mode: $ENV_MODE)..."
    echo ""
    
    check_prerequisites
    setup_env_file
    setup_directories
    start_influxdb
    setup_influxdb_buckets
    start_mlflow
    install_dependencies
    run_health_checks
    print_summary
}

main "$@"
