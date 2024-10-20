# Deactivate any active conda environment
# conda deactivate

# Set PYTHONPATH to the directory containing your Python packages, if needed
# It's unusual to set PYTHONPATH to a bin directory, so this might be unnecessary
export PATH=/home/skumar/DaatScience/AutomatedTrading:$PATH

# Use the specified Python interpreter for the poetry environment
poetry env use /home/skumar/DaatScience/AutomatedTrading/.venv/bin/python3.8

# Activate the poetry-managed virtual environment
poetry shell


