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
git commit -m "Initial commit: SHMU radar data processor

Complete Python toolkit for processing Slovak Hydrometeorological Institute (SHMU) radar data:

✨ Features:
- Automatic data download from SHMU API
- HDF5 to JavaScript-ready JSON conversion  
- Support for ZMAX, CAPPI 2km, and precipitation products
- Precipitation rate estimation from reflectivity
- Map visualization examples
- Complete documentation and usage examples

🎯 Ready for frontend integration with mapping libraries
📊 Processes multi-radar composite data covering Slovakia region
⚡ Optimized data structures for web applications"

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