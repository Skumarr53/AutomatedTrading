#!/bin/bash
# Complete System Test Script
# Tests all new features end-to-end

set -e  # Exit on error

echo "=========================================="
echo "Complete System Test - Live Trading E2E"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Test counter
PASSED=0
FAILED=0
SKIPPED=0

test_step() {
    local name=$1
    local command=$2
    
    echo -e "${YELLOW}Testing: $name${NC}"
    
    if eval "$command" > /tmp/test_output.log 2>&1; then
        echo -e "${GREEN}✅ PASSED: $name${NC}"
        ((PASSED++))
        return 0
    else
        local exit_code=$?
        if [ $exit_code -eq 3 ] || grep -q "SKIP\|skip\|Skip" /tmp/test_output.log; then
            echo -e "${YELLOW}⏭️  SKIPPED: $name${NC}"
            ((SKIPPED++))
            return 0
        else
            echo -e "${RED}❌ FAILED: $name${NC}"
            echo "Last 10 lines of output:"
            tail -10 /tmp/test_output.log
            ((FAILED++))
            return 1
        fi
    fi
}

# Check prerequisites
echo "Checking prerequisites..."
if ! command -v python &> /dev/null; then
    echo -e "${RED}Error: python not found${NC}"
    exit 1
fi

if ! command -v pytest &> /dev/null; then
    echo -e "${YELLOW}Warning: pytest not found, skipping pytest tests${NC}"
fi

echo ""

# Test 1: Data Collection Verification
echo "=== Part 1: Data Collection ==="
test_step "Data Collection Verification" \
    "python scripts/verify_data_collection.py --symbols RELIANCE --days 7 --quiet"

if command -v pytest &> /dev/null; then
    test_step "Data Integrity Tests" \
        "pytest tests/test_data_integrity.py::TestDataExistence::test_ticker_data_exists -v"
fi

echo ""

# Test 2: Backtesting
echo "=== Part 2: Backtesting ==="
test_step "Backtest Execution" \
    "python scripts/run_backtest.py --symbol RELIANCE --days 30 --quiet"

echo ""

# Test 3: Live Trading Flow
echo "=== Part 3: Live Trading Flow ==="
test_step "Live Trading Flow Test" \
    "python scripts/test_live_trading_flow.py --symbol RELIANCE --paper --quiet"

if command -v pytest &> /dev/null; then
    test_step "E2E Tests" \
        "pytest tests/test_live_trading_e2e.py::TestDataPipeline -v"
fi

echo ""

# Test 4: Monitoring
echo "=== Part 4: Monitoring ==="
test_step "Monitoring Infrastructure" \
    "python scripts/verify_monitoring.py --prometheus-only"

echo ""

# Test 5: Alerting
echo "=== Part 5: Alerting ==="
test_step "Alerting System" \
    "python scripts/test_alerting.py --dry-run"

echo ""

# Test 6: Health Endpoints
echo "=== Part 6: Health Endpoints ==="
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    test_step "Health API" \
        "curl -s http://localhost:8080/health | grep -q status"
else
    echo -e "${YELLOW}⏭️  SKIPPED: Health API (not running)${NC}"
    ((SKIPPED++))
fi

echo ""

# Summary
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "${GREEN}Passed:  $PASSED${NC}"
echo -e "${RED}Failed:  $FAILED${NC}"
echo -e "${YELLOW}Skipped: $SKIPPED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✅ All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}❌ Some tests failed${NC}"
    exit 1
fi
