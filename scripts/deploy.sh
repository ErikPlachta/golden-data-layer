#!/bin/bash
# Deploy Golden Data Layer to SQL Server
# Usage: ./scripts/deploy.sh -S localhost,1433 -U SA -P 'YourPass'

set -e

SERVER=""
USER=""
PASS=""

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
