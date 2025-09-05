#!/bin/bash
# Git setup script for imeteo-radar repository

echo "🚀 Setting up Git repository for imeteo-radar"
echo "=============================================="

# Initialize git repository
git init

# Set main branch
git branch -M main

# Add all files (respecting .gitignore)
git add .

# Initial commit
git commit -m "Initial commit: SHMU radar data processor MVP

- MVP processor for SHMU radar data
- JavaScript frontend integration
- Support for ZMAX, CAPPI 2km, and precipitation products  
- Complete documentation and examples
- Visualization demos"

echo ""
echo "✅ Git repository initialized successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Create 'imeteo-radar' repository on GitHub (private)"
echo "2. Copy the remote URL from GitHub"
echo "3. Run: git remote add origin <YOUR_REPO_URL>"
echo "4. Run: git push -u origin main"
echo ""
echo "Example:"
echo "git remote add origin https://github.com/YOUR_USERNAME/imeteo-radar.git"
echo "git push -u origin main"