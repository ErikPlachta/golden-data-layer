#!/bin/bash
# Deploy Golden Data Layer to SQL Server
# Usage: ./scripts/deploy.sh
#   Reads DB_SERVER, DB_USER, DB_PASSWORD from .env (project root)
#   CLI flags -S, -U, -P override .env values

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Load .env if present
SERVER=""
USER=""
PASS=""
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
    SERVER="${DB_SERVER:-}"
    USER="${DB_USER:-}"
    PASS="${DB_PASSWORD:-}"
fi

# CLI flags override .env
while [[ $# -gt 0 ]]; do
    case $1 in
        -S) SERVER="$2"; shift 2 ;;
        -U) USER="$2"; shift 2 ;;
        -P) PASS="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ -z "$SERVER" || -z "$USER" || -z "$PASS" ]]; then
    echo "Usage: $0 -S server -U user -P password"
    echo "  Or set DB_SERVER, DB_USER, DB_PASSWORD in .env"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)/sql"
FILES=(
    "01_ddl.sql"
    "02_meta_programmability.sql"
    "03_audit.sql"
    "04_silver.sql"
    "05_seed_data.sql"
    "06_gold.sql"
)

# Check if database already exists
DB_EXISTS=$(sqlcmd -S "$SERVER" -U "$USER" -P "$PASS" -h -1 -W \
    -Q "SET NOCOUNT ON; SELECT CASE WHEN DB_ID('GoldenDataLayer') IS NOT NULL THEN '1' ELSE '0' END" 2>/dev/null | tr -d '[:space:]')

if [[ "$DB_EXISTS" == "1" ]]; then
    echo "Database 'GoldenDataLayer' already exists on $SERVER."
    echo "  1) Drop and recreate (destroys all data)"
    echo "  2) Exit"
    read -rp "Choose [1/2]: " choice
    case $choice in
        1)
            echo "Dropping GoldenDataLayer..."
            sqlcmd -S "$SERVER" -U "$USER" -P "$PASS" -b \
                -Q "ALTER DATABASE GoldenDataLayer SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE GoldenDataLayer"
            echo "  Dropped."
            ;;
        *)
            echo "Exiting."
            exit 0
            ;;
    esac
fi

echo "=== Deploying Golden Data Layer ==="
echo "Server: $SERVER"
echo ""

for f in "${FILES[@]}"; do
    echo "→ Running $f ..."
    sqlcmd -S "$SERVER" -U "$USER" -P "$PASS" -i "$SCRIPT_DIR/$f" -b
    echo "  ✓ $f complete"
done

echo ""
echo "=== Deployment complete ==="
echo "Run pipeline: sqlcmd -S $SERVER -U $USER -P '$PASS' -Q 'EXEC dbo.usp_run_full_pipeline'"
