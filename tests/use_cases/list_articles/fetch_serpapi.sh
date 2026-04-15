#!/bin/sh

SCRIPT_DIR=$(dirname "$0")
ENV_FILE="$SCRIPT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

. "$ENV_FILE"

if [ -z "$SERPAPI_KEY" ]; then
    echo "Error: SERPAPI_KEY is not set in .env"
    exit 1
fi

if [ -z "$SERPAPI_AUTHOR" ]; then
    SERPAPI_AUTHOR="$OPENALEX_AUTHOR"
fi

DATA_DIR="$SCRIPT_DIR/data"
mkdir -p "$DATA_DIR"
OUTPUT_FILE="$DATA_DIR/serpapi_scholar.json"
STDERR_FILE="$DATA_DIR/error.log"

Q=$(printf "%s" "$SERPAPI_AUTHOR" | sed 's/ /+/g')
URL="https://serpapi.com/search.json?engine=google_scholar&q=$Q&api_key=$SERPAPI_KEY"

echo "Fetching Google Scholar (via SerpAPI) for '$SERPAPI_AUTHOR'"

curl -s -L "$URL" 2>"$STDERR_FILE" > "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
    echo "✓ SerpAPI results saved to $OUTPUT_FILE"
else
    echo "✗ Error fetching SerpAPI. Check $STDERR_FILE"
    exit 1
fi
