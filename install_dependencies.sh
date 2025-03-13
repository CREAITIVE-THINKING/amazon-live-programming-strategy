#!/bin/bash

echo "Installing dependencies for Amazon Live Programming Strategy Analysis..."

# Install Python dependencies from requirements.txt
pip install -r requirements.txt

# Install additional dependencies for PDF generation
pip install mdpdf

# Install system dependencies if needed (optional)
# For macOS, you might need Homebrew packages
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "macOS detected, checking for Homebrew..."
    if ! command -v brew &> /dev/null; then
        echo "Homebrew not found, but is recommended for installing system dependencies."
        echo "Visit https://brew.sh to install Homebrew if needed."
    else
        echo "Homebrew found, installing system dependencies..."
        # Add any Homebrew packages needed here
        # brew install pkg-config cairo pango libpng jpeg giflib
    fi
fi

echo "All dependencies installed!"
echo "You can now run the analysis with:"
echo "  python src/generate_pivot_tables.py"
echo "  python src/generate_programming_strategy.py" 