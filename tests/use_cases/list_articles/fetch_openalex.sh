#!/bin/sh

SCRIPT_DIR=$(dirname "$0")
ENV_FILE="$SCRIPT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

. "$ENV_FILE"

if [ -z "$OPENALEX_AUTHOR" ]; then
    echo "Error: OPENALEX_AUTHOR is not set in .env"
    exit 1
fi

if [ -z "$OPENALEX_YEAR_START" ] || [ -z "$OPENALEX_YEAR_END" ]; then
    echo "Error: OPENALEX_YEAR_START / OPENALEX_YEAR_END must be set in .env"
    exit 1
fi

DATA_DIR="$SCRIPT_DIR/data"
mkdir -p "$DATA_DIR"
OUTPUT_FILE="$DATA_DIR/openalex_works.json"
STDERR_FILE="$DATA_DIR/error.log"


# URL-encode the author name (simple space->%20) for the query
Q=$(printf "%s" "$OPENALEX_AUTHOR" | sed 's/ /%20/g')

# Use OpenAlex works API with date range filters
FROM_DATE="${OPENALEX_YEAR_START}-01-01"
UNTIL_DATE="${OPENALEX_YEAR_END}-12-31"
URL="https://api.openalex.org/works?filter=raw_author_name.search:$Q,from_publication_date:$FROM_DATE,to_publication_date:$UNTIL_DATE&per-page=200"

echo "Fetching OpenAlex works for '$OPENALEX_AUTHOR' ($OPENALEX_YEAR_START-$OPENALEX_YEAR_END)"
echo "Request URL: $URL"

curl -s -L "$URL" 2>"$STDERR_FILE" > "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
    echo "✓ OpenAlex results saved to $OUTPUT_FILE"
else
    echo "✗ Error fetching OpenAlex. Check $STDERR_FILE"
    exit 1
fi
