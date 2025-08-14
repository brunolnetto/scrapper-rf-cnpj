# Justfile for Receita Federal CNPJ ETL project

# Default recipe to show help
default:
    @just --list

# Show detailed help information
help:
    @echo "Receita Federal CNPJ ETL Project - Available Commands:"
    @echo ""
    @echo "ETL Commands:"
    @echo "  run-current     Run ETL for current year/month"
    @echo "  run-etl <year> <month>  Run ETL for specific year/month"
    @echo "  run             Alias for run-current"
    @echo ""
    @echo "Development Commands:"
    @echo "  install         Install Python dependencies using uv"
    @echo "  env             Create virtual environment"
    @echo "  lint            Run ruff linter with auto-fix"
    @echo ""
    @echo "Maintenance Commands:"
    @echo "  clean           Remove logs and cache files"
    @echo "  clean-logs      Remove build artifacts and log files"
    @echo "  clean-cache     Remove cache directories"
    @echo ""
    @echo "Utility Commands:"
    @echo "  search <token>  Search for token in codebase"
    @echo "  replace <old> <new>  Replace token in codebase"
    @echo "  minimal-requirements  Generate minimal requirements file"
    @echo ""
    @echo "Examples:"
    @echo "  just run-current                    # Run ETL for current date"
    @echo "  just run-etl 2023 7                 # Run ETL for July 2023"
    @echo "  just search \"POSTGRES_DBNAME\"       # Search for database name"
    @echo "  just replace \"old_host\" \"new_host\"  # Replace host in code"
    @echo "  just clean                          # Clean all artifacts"

# Removes log info
clean-logs:
    rm -fr build/ dist/ .eggs/
    find . -name '*.log' -o -name '*.log' -exec rm -fr {} +

# Remove cache artifacts
clean-cache:
    find . -name '*cache*' -exec rm -rf {} +

# Add a rule to remove unnecessary assets
clean: clean-logs clean-cache

# Creates a virtual environment
env:
    pip install virtualenv
    virtualenv .venv

# Installs the python requirements
install:
    uv pip install -r requirements.txt

# Searchs for a token in the code
search token:
    grep -rnw . \
    --exclude-dir=venv \
    --exclude-dir=.git \
    --exclude=poetry.lock \
    -e "{{token}}"

# Replaces a token in the code
replace token new_token:
    sed -i 's/{{token}}/{{new_token}}/g' $(grep -rl "{{token}}" . \
        --exclude-dir=venv \
        --exclude-dir=.git \
        --exclude=poetry.lock)

# Generates minimal requirements
minimal-requirements:
    python3 scripts/clean_packages.py requirements.txt requirements.txt

# Check lint 
check:
    ruff check src

# Perform inplace lint fixes
lint:
    ruff check --fix src

# Run the application
run:
    python3 ./src/main.py

# Run ETL with specific year and month
run-etl year month:
    python3 ./src/main.py --year {{year}} --month {{month}}

# Run ETL for current date
run-current:
    python3 ./src/main.py 