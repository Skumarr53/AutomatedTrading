#!/bin/bash
# Activate the conda environment
conda ~/miniconda3/bin/activate algotrade

# Change to the desired directory
cd /home/skumar/DaatScience/AutomatedTrading

python scripts/OrderBook_backup.py
# python scripts/OrderBook_validation.py