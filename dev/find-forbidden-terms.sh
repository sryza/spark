#!/usr/bin/env bash

# Script to find forbidden terms in git diff
# Compares current working state with the last non-SDP commit
# and searches for terms listed in dev/forbidden-terms.txt

set -e

LAST_NON_SDP_COMMIT=ba7a537225a7891f3e0d650d4141ef7bb8d06368

# Check if forbidden-terms.txt exists
if [ ! -f "dev/forbidden-terms.txt" ]; then
  echo "Error: dev/forbidden-terms.txt does not exist"
  exit 1
fi

# Check if file is empty
if [ ! -s "dev/forbidden-terms.txt" ]; then
  echo "Warning: dev/forbidden-terms.txt is empty. No terms to search for."
  exit 0
fi

# Step 1: Create the diff, excluding dev/forbidden-terms.txt
diff_output=$(
  git diff $LAST_NON_SDP_COMMIT -- ':!dev/forbidden-terms.txt' \
  | awk '/^diff --git/ {fname = substr($NF, 3); next} {print fname ": " $0}' \
  | grep -v "^python/pyspark/sql/connect/proto/" \
  | grep -v "^python/pyspark/sql/pipelines/proto/" \
)

# If there's no diff, exit
if [ -z "$diff_output" ]; then
  echo "No changes detected between current state and commit $LAST_NON_SDP_COMMIT"
  exit 0
fi

# Create a temporary file for the diff
diff_file=$(mktemp)
echo "$diff_output" > "$diff_file"

echo "Searching for forbidden terms in diff..."
echo "----------------------------------------"

found_terms=0

# Step 2: Search for each forbidden term in the diff
while IFS= read -r term; do
  # Skip empty lines
  [ -z "$term" ] && continue
  
  # Search for the term in added lines only (case insensitive)
  # Only look at lines starting with '+' but not '+++' (which are file headers)
  matches=$(grep -i "$term" "$diff_file" | grep " +" | grep -v " +++" || true)
  
  if [ -n "$matches" ]; then
    echo "Found forbidden term: '$term'"
    echo "$matches"
    echo ""
    found_terms=1
  fi
done < "dev/forbidden-terms.txt"

# Clean up
rm "$diff_file"

if [ $found_terms -eq 0 ]; then
  echo "No forbidden terms found in the diff."
  exit 0
else
  echo "Found forbidden terms in the diff. Please review and fix."
  exit 1
fi
