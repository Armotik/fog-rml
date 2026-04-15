#!/bin/sh

SCRIPT_DIR=$(dirname "$0")
ENV_FILE="$SCRIPT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

. "$ENV_FILE"

# Run each fetcher where available
sh "$SCRIPT_DIR/fetch_openalex.sh" || exit 1
sh "$SCRIPT_DIR/fetch_hal.sh" || exit 1
sh "$SCRIPT_DIR/fetch_dblp.sh" || exit 1
sh "$SCRIPT_DIR/fetch_serpapi.sh" || exit 1

echo "All fetchers completed. Data under $SCRIPT_DIR/data"
