#!/bin/bash
# Shell script to run the Direct Nitter Twitter ETL pipeline
# Can be used with cron for scheduling

# Set the script directory as the working directory
cd "$(dirname "$0")"

# Log file for capturing script output
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/direct_nitter_cron_$(date +"%Y%m%d_%H%M%S").log"

# Start logging
echo "===== Direct Nitter Twitter ETL Pipeline =====" > "$LOG_FILE"
echo "Started at: $(date)" >> "$LOG_FILE"

# Check if virtual environment exists and activate it
if [ -d ".venv" ]; then
    echo "Activating virtual environment..." >> "$LOG_FILE"
    source .venv/bin/activate
elif [ -d "venv" ]; then
    echo "Activating virtual environment..." >> "$LOG_FILE"
    source venv/bin/activate
else
    echo "No virtual environment found. Continuing with system Python..." >> "$LOG_FILE"
fi

# Load environment variables if .env file exists
if [ -f ".env" ]; then
    echo "Loading environment variables from .env file..." >> "$LOG_FILE"
    set -a
    source .env
    set +a
fi

# Run the ETL pipeline
echo "Running Direct Nitter Twitter ETL pipeline..." >> "$LOG_FILE"
./run_direct_nitter_etl.py --signal financial_tweets_direct_nitter >> "$LOG_FILE" 2>&1

# Check the exit status
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo "ETL pipeline completed successfully." >> "$LOG_FILE"
else
    echo "ETL pipeline failed with exit code $EXIT_CODE." >> "$LOG_FILE"
fi

# Deactivate virtual environment if it was activated
if [ -n "$VIRTUAL_ENV" ]; then
    echo "Deactivating virtual environment..." >> "$LOG_FILE"
    deactivate
fi

echo "Finished at: $(date)" >> "$LOG_FILE"
echo "===== End of Direct Nitter Twitter ETL Pipeline =====" >> "$LOG_FILE"

exit $EXIT_CODE 