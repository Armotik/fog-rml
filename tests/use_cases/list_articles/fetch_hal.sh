#!/bin/sh

SCRIPT_DIR=$(dirname "$0")
ENV_FILE="$SCRIPT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

. "$ENV_FILE"

if [ -z "$HAL_AUTHOR" ]; then
    echo "Error: HAL_AUTHOR is not set in .env"
    exit 1
fi

DATA_DIR="$SCRIPT_DIR/data"
mkdir -p "$DATA_DIR"
OUTPUT_FILE="$DATA_DIR/hal_results.json"
STDERR_FILE="$DATA_DIR/error.log"

Q=$(printf "%s" "$HAL_AUTHOR" | sed 's/ /+/g')
URL="https://api.archives-ouvertes.fr/search/?q=authFullName_t:$Q&wt=json&rows=100"

echo "Fetching HAL records for '$HAL_AUTHOR'"

curl -s -L "$URL" 2>"$STDERR_FILE" > "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
    echo "✓ HAL results saved to $OUTPUT_FILE"
else
    echo "✗ Error fetching HAL. Check $STDERR_FILE"
    exit 1
fi
