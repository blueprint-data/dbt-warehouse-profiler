#!/bin/bash

set -e

echo "====================================="
echo "Setting up dbt-warehouse-profiler locally"
echo "====================================="

# Check Python installation
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
echo "✓ Python ${PYTHON_VERSION} detected"

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip setuptools wheel

# Install dbt-core
echo "Installing dbt-core..."
pip install "dbt-core>=1.10.0,<3.0.0"

# Ask which adapter to install
echo ""
echo "Which database adapter would you like to install?"
echo "1) BigQuery (default)"
echo "2) Snowflake"
echo "3) PostgreSQL"
echo "4) All adapters"
read -p "Enter choice [1-4] (default: 1): " ADAPTER_CHOICE
ADAPTER_CHOICE=${ADAPTER_CHOICE:-1}

case $ADAPTER_CHOICE in
    1)
        echo "Installing dbt-bigquery..."
        pip install "dbt-bigquery>=1.8.0"
        ;;
    2)
        echo "Installing dbt-snowflake..."
        pip install "dbt-snowflake>=1.8.0"
        ;;
    3)
        echo "Installing dbt-postgres..."
        pip install "dbt-postgres>=1.8.0"
        ;;
    4)
        echo "Installing all adapters..."
        pip install "dbt-bigquery>=1.8.0"
        pip install "dbt-snowflake>=1.8.0"
        pip install "dbt-postgres>=1.8.0"
        ;;
    *)
        echo "Invalid choice. Installing dbt-bigquery as default..."
        pip install "dbt-bigquery>=1.8.0"
        ;;
esac

echo "✓ Dependencies installed"

# Setup profiles.yml
PROFILES_DIR="$HOME/.dbt"
PROFILES_FILE="$PROFILES_DIR/profiles.yml"

if [ ! -f "$PROFILES_FILE" ]; then
    echo ""
    echo "Creating profiles.yml directory..."
    mkdir -p "$PROFILES_DIR"
    echo "✓ Profiles directory created: $PROFILES_DIR"
    
    echo ""
    echo "Do you want to create a basic profiles.yml template?"
    read -p "[y/N] (default: N): " CREATE_PROFILES
    CREATE_PROFILES=${CREATE_PROFILES:-N}
    
    if [[ "$CREATE_PROFILES" =~ ^[Yy]$ ]]; then
        cat > "$PROFILES_FILE" << 'EOF'
dbt_warehouse_profiler:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: your-project-id
      dataset: your_dataset_name
      threads: 4
      timeout_seconds: 300
      location: US
EOF
        echo "✓ Created profiles.yml at $PROFILES_FILE"
        echo "  Please update with your BigQuery credentials"
    else
        echo "Skipping profiles.yml creation"
        echo "  You can create it later at: $PROFILES_FILE"
    fi
else
    echo "✓ profiles.yml already exists at $PROFILES_FILE"
fi

# Setup integration tests
echo ""
echo "Setting up integration tests..."
cd integration_tests

# Install dependencies
echo "Installing package dependencies..."
dbt deps

cd ..
echo "✓ Integration tests configured"

# Validate the package
echo ""
echo "Validating dbt project..."
dbt parse
echo "✓ Package validation successful"

echo ""
echo "====================================="
echo "Setup complete! 🎉"
echo "====================================="
echo ""
echo "To activate the virtual environment:"
echo "  source venv/bin/activate"
echo ""
echo "To deactivate the virtual environment:"
echo "  deactivate"
echo ""
echo "Next steps:"
echo "  1. Update your profiles.yml with database credentials"
echo "  2. Test the macros:"
echo "     dbt run-operation dbt_warehouse_profiler.list_database_schemas"
echo ""
echo "For more information, see the README.md file"
