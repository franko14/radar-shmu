#!/bin/sh
# Install git hooks from scripts/hooks/ into .git/hooks/
# Run once after cloning: ./scripts/install-hooks.sh

set -e

if ! git rev-parse --git-dir > /dev/null 2>&1; then
  echo "ERROR: Not inside a git repository"
  exit 1
fi

HOOKS_SRC="$(cd "$(dirname "$0")/hooks" && pwd)"
HOOKS_DST="$(git rev-parse --git-dir)/hooks"

count=0
for hook in pre-commit pre-push commit-msg; do
  if [ -f "$HOOKS_SRC/$hook" ]; then
    cp "$HOOKS_SRC/$hook" "$HOOKS_DST/$hook"
    chmod +x "$HOOKS_DST/$hook"
    echo "Installed $hook"
    count=$((count + 1))
  else
    echo "WARNING: $HOOKS_SRC/$hook not found, skipping"
  fi
done

if [ "$count" -eq 0 ]; then
  echo "ERROR: No hooks found in $HOOKS_SRC"
  exit 1
fi

echo "Done. Installed $count hook(s) to $HOOKS_DST"
