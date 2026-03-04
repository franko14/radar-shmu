#!/bin/sh
# Interactive release checklist for iMeteo Radar.
# Guides you through the git-flow release process — does NOT auto-edit files.
#
# Usage: ./scripts/release.sh

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { printf "${GREEN}[OK]${NC} %s\n" "$1"; }
warn()  { printf "${YELLOW}[!!]${NC} %s\n" "$1"; }
error() { printf "${RED}[ERR]${NC} %s\n" "$1"; exit 1; }

# --- Pre-checks ---
branch=$(git symbolic-ref --short HEAD 2>/dev/null)
if [ "$branch" != "main" ]; then
  error "You must be on 'main' branch (currently on '$branch')"
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  error "Working tree is not clean. Commit or stash changes first."
fi

git fetch origin --tags --quiet
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main 2>/dev/null)" ]; then
  warn "Local main is not up to date with origin/main. Consider: git pull"
fi

# --- Read current version ---
current_version=$(grep -m1 '^version' pyproject.toml | sed 's/.*"\(.*\)"/\1/')
info "Current version: $current_version"

# Parse semver
major=$(echo "$current_version" | cut -d. -f1)
minor=$(echo "$current_version" | cut -d. -f2)
patch=$(echo "$current_version" | cut -d. -f3)

# --- Prompt for bump type ---
echo ""
echo "Which version bump?"
echo "  1) patch  → $major.$minor.$((patch + 1))"
echo "  2) minor  → $major.$((minor + 1)).0"
echo "  3) major  → $((major + 1)).0.0"
printf "Choice [1/2/3]: "
read choice

case "$choice" in
  1) new_version="$major.$minor.$((patch + 1))" ;;
  2) new_version="$major.$((minor + 1)).0" ;;
  3) new_version="$((major + 1)).0.0" ;;
  *) error "Invalid choice" ;;
esac

info "New version: $new_version"
echo ""

# --- Create release branch ---
release_branch="release/v$new_version"
printf "Create branch '${release_branch}'? [y/N]: "
read confirm
if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
  echo "Aborted."
  exit 0
fi

git checkout -b "$release_branch"
info "Created branch: $release_branch"

# --- Manual edits ---
echo ""
echo "=== Manual steps ==="
echo ""
echo "1. Edit pyproject.toml:"
echo "   version = \"$new_version\""
echo ""
echo "2. Edit CHANGELOG.md:"
echo "   Add ## [$new_version] - $(date +%Y-%m-%d) section"
echo ""
printf "Press Enter when edits are done..."
read _

# --- Verify edits ---
problems=0

file_version=$(grep -m1 '^version' pyproject.toml | sed 's/.*"\(.*\)"/\1/')
if [ "$file_version" != "$new_version" ]; then
  warn "pyproject.toml still shows version '$file_version' (expected '$new_version')"
  problems=1
fi

if ! grep -q "\[${new_version}\]" CHANGELOG.md; then
  warn "CHANGELOG.md does not contain a [$new_version] entry"
  problems=1
fi

if [ "$problems" -ne 0 ]; then
  printf "Continue anyway? [y/N]: "
  read cont
  if [ "$cont" != "y" ] && [ "$cont" != "Y" ]; then
    echo "Aborted. You're still on branch $release_branch."
    exit 0
  fi
fi

# --- Commit and push ---
git add pyproject.toml CHANGELOG.md
git commit -m "release: v$new_version"
git push -u origin "$release_branch"
info "Pushed $release_branch to origin"

# --- Create PR ---
echo ""
echo "Create the PR:"
echo ""
echo "  gh pr create --title \"release: v$new_version\" --body \"Bump version to $new_version and update changelog.\""
echo ""

# --- Post-merge instructions ---
echo "After PR is merged:"
echo ""
echo "  git checkout main && git pull"
echo "  git tag v$new_version && git push origin v$new_version"
echo ""
echo "CI will automatically build and push the Docker image on tag push."
echo ""
info "Release checklist complete."
