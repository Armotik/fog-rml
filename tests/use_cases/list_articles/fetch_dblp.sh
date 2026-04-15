#!/bin/sh

SCRIPT_DIR=$(dirname "$0")
ENV_FILE="$SCRIPT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

. "$ENV_FILE"

if [ -z "$DBLP_AUTHOR" ]; then
    echo "Error: DBLP_AUTHOR is not set in .env"
    exit 1
fi

DATA_DIR="$SCRIPT_DIR/data"
mkdir -p "$DATA_DIR"
OUTPUT_FILE="$DATA_DIR/dblp_results.json"
STDERR_FILE="$DATA_DIR/error.log"

Q=$(printf "%s" "$DBLP_AUTHOR" | sed 's/ /+/g')
URL="https://dblp.org/search/publ/api?q=$Q&format=json"

echo "Fetching DBLP publications for '$DBLP_AUTHOR'"

curl -s -L "$URL" 2>"$STDERR_FILE" > "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
    echo "✓ DBLP results saved to $OUTPUT_FILE"
else
    echo "✗ Error fetching DBLP. Check $STDERR_FILE"
    exit 1
fi
