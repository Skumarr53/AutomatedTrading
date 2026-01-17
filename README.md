# AutomatedTrading System

Automated trading system with distributed data processing, ML model training, and real-time monitoring.


Imnporntant Fix:
``` python
   ## added below to handle error at _get_metadata_for_step of sklearn.imblearn.pipeline.Pipeline
   ## under hasattr(cloned_transformer, "transform") or hasattr(
   ## cloned_transformer, "fit_transform") block
   if isinstance(X, tuple):
      X, y = X
```

## Features

- **Distributed Processing**: Ray-based parallel processing for 100+ symbols
- **Time-Series Database**: InfluxDB for high-performance data storage
- **ML Model Training**: Sklearn-based pipelines with MLflow tracking
- **Monitoring Dashboard**: Grafana-based real-time monitoring
- **Automated Backups**: Scheduled backups with retention policy
- **Health Monitoring**: Container health checks and auto-start
- **Slack Notifications**: Real-time alerts and notifications

## Quick Start

### Prerequisites

- Podman and podman-compose
- Python 3.11+
- uv (Python package manager)

### Setup

1. **Clone repository**:
   ```bash
   git clone <repository-url>
   cd AutomatedTrading
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Create data directories**:
   ```bash
   ./scripts/ensure_data_directories.sh
   ```

5. **Start services**:
   ```bash
   podman-compose up -d
   ```

6. **Run application**:
   ```bash
   python main.py
   ```

The application will automatically:
- Check and start required containers
- Initialize Ray cluster (if distributed mode enabled)
- Start health API server
- Begin data collection and trading

## Data Storage

All persistent data is stored in `./data/` (project folder, not OS partition):

- `data/influxdb/` - InfluxDB database
- `data/mlflow/` - MLflow models and experiments
- `data/grafana/` - Grafana dashboards
- `data/backups/` - Automated backups
- `data/logs/` - Application logs

## Services

| Service | Port | URL |
|---------|------|-----|
| InfluxDB | 8086 | http://localhost:8086 |
| MLflow | 5000 | http://localhost:5000 |
| Grafana | 3000 | http://localhost:3000 |
| Health API | 8080 | http://localhost:8080/health |

## Documentation

- [Infrastructure Guide](docs/INFRASTRUCTURE.md) - Container management and setup
- [Backup Guide](docs/BACKUP_GUIDE.md) - Backup and restore procedures
- [Monitoring Guide](docs/MONITORING_GUIDE.md) - Grafana dashboard usage
- [Slack Setup](docs/SLACK_SETUP.md) - Slack notification configuration
- [Parallel Processing](docs/PARALLEL_PROCESSING.md) - Ray vs Joblib architecture

## Key Features

### Distributed Processing

- **Ray**: Distributed data ingestion and signal generation (100+ symbols)
- **Joblib**: Parallel model training (sklearn-optimized)
- See [PARALLEL_PROCESSING.md](docs/PARALLEL_PROCESSING.md) for details

### Data Management

- **Primary Storage**: InfluxDB (time-series optimized)
- **Backup**: Optional CSV backup (disabled by default)
- **Training Data**: Loads directly from InfluxDB

### Monitoring

- **Grafana Dashboard**: Real-time system and trading metrics
- **Health API**: `/health`, `/healthz`, `/ready`, `/metrics`
- **Prometheus Metrics**: Exposed at `/metrics`

### Automated Operations

- **Container Health Checks**: Auto-start on application launch
- **Automated Backups**: Daily full + hourly incremental
- **Slack Notifications**: Real-time alerts

## Configuration

### Environment Variables

See `.env.example` for all available environment variables.

Key variables:
- `INFLUXDB_TOKEN` - InfluxDB authentication token
- `SLACK_WEBHOOK_URL` - Slack webhook for notifications
- `RAY_ENABLED` - Enable/disable distributed mode

### Configuration Files

- `src/config/config.yaml` - Main configuration
- `src/config/custom.yaml` - Custom overrides
- `compose.yml` - Container orchestration

## Backup System

### Manual Backup

```bash
# Full backup
python scripts/backup_data.py --full

# Incremental backup
python scripts/backup_data.py --incremental

# Cleanup old backups
python scripts/backup_data.py --cleanup
```

### Automated Backups

```bash
# Install cron jobs (daily full + hourly incremental)
./scripts/schedule_backups.sh install
```

See [BACKUP_GUIDE.md](docs/BACKUP_GUIDE.md) for details.

## Monitoring

### Access Grafana

1. Start services: `podman-compose up -d`
2. Open: http://localhost:3000
3. Login: `admin` / `admin` (change on first login)

### View Metrics

- **Health API**: http://localhost:8080/health
- **Prometheus Metrics**: http://localhost:8080/metrics
- **Grafana Dashboard**: http://localhost:3000

See [MONITORING_GUIDE.md](docs/MONITORING_GUIDE.md) for dashboard usage.

## Troubleshooting

### Ray Initialization Error

The system now handles Ray connection failures gracefully:
- Tries to connect to remote cluster
- Falls back to local cluster if connection fails
- Logs clear error messages

### Container Issues

```bash
# Check container status
./scripts/check_containers.sh

# View logs
podman logs trading-influxdb
podman logs trading-mlflow
```

### Data Issues

```bash
# Verify InfluxDB data
python scripts/verify_influxdb_data.py

# Inspect InfluxDB contents
python scripts/inspect_influxdb.py
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Quality

```bash
# Type checking
mypy src/

# Linting
ruff check src/
```

## Architecture

```
┌─────────────────────────────────────────┐
│         Trading Application             │
│  (main.py - MarketAnalysisApp)          │
└──────────────┬──────────────────────────┘
               │
    ┌──────────┴──────────┐
    │                     │
┌───▼──────┐      ┌───────▼──────┐
│   Ray    │      │   Joblib     │
│(Distributed)│   │ (Local CPU)  │
│ Data/Signals│   │ Model Training│
└───────────┘      └──────────────┘
    │                     │
    └──────────┬──────────┘
               │
    ┌──────────▼──────────┐
    │      InfluxDB       │
    │  (Time-Series DB)   │
    └─────────────────────┘
```

## License

[Your License Here]

## Support

For issues and questions:
- Check documentation in `docs/`
- Review logs in `data/logs/`
- Check container health: `./scripts/check_containers.sh`
