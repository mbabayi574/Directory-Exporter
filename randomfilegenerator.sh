#!/bin/bash
# =============================================================================
# Script: create_stream_files.sh
# Description: Creates empty files in a random directory structure every
#              6 seconds - 1 minute.
# Directory pattern: streams/{random_name}/nodes/{random_name}/input
# =============================================================================

# --- Configuration ---
BASE_DIR="${BASE_DIR:-.}"              # Base directory (default: current dir)
MIN_INTERVAL=6                         # Minimum interval in seconds (6 sec)
MAX_INTERVAL=60                        # Maximum interval in seconds (1 min)
FILE_PREFIX="stream_"                   # Prefix for generated files

# --- Helper Functions ---

# Generate a random alphanumeric name (default 8 chars)
generate_random_name() {
    local length="${1:-8}"
    tr -dc 'A-Za-z0-9_' < /dev/urandom | head -c "$length"
}

# Generate a random integer between min and max (inclusive)
random_interval() {
    local min="$1"
    local max="$2"
    echo $(( min + RANDOM % (max - min + 1) ))
}

# Create the directory structure and empty file
create_stream_file() {
    local stream_name
    local node_name
    local target_dir
    local timestamp
    local filename

    stream_name="$(generate_random_name 10)"
    node_name="$(generate_random_name 12)"
    target_dir="${BASE_DIR}/streams/${stream_name}/nodes/${node_name}/input"

    # Create directories (including parents)
    mkdir -p "$target_dir"

    # Generate a timestamped filename
    timestamp=$(date '+%Y%m%d_%H%M%S')
    filename="${FILE_PREFIX}${timestamp}_$(generate_random_name 6)"

    # Create empty file
    touch "${target_dir}/${filename}"

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Created: ${target_dir}/${filename}"
}

# --- Main Loop ---

echo "============================================================"
echo " Stream File Generator"
echo "============================================================"
echo " Base directory : $(cd "$BASE_DIR" 2>/dev/null && pwd || echo "$BASE_DIR")"
echo " Interval range : ${MIN_INTERVAL}s - ${MAX_INTERVAL}s (6s - 1min)"
echo " Press Ctrl+C to stop"
echo "============================================================"
echo ""

# Seed RANDOM with current time for better randomness
RANDOM=$(date +%s)

while true; do
    create_stream_file

    # Calculate next interval
    next_interval=$(random_interval "$MIN_INTERVAL" "$MAX_INTERVAL")
    next_min=$(awk 'BEGIN {printf "%.1f", '"$next_interval"'/60}')

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Next file in ~${next_min} minutes..."
    echo ""

    sleep "$next_interval"
done
