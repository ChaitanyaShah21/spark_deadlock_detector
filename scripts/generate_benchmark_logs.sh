#!/usr/bin/env bash
# Generate the synthetic benchmark logs into data/generated/.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/generated
for lines in 5k:2499 20k:9999 50k:24999 200k:99999; do
    name=${lines%%:*} steps=${lines##*:}
    "${PYTHON:-python3}" scripts/generate_log.py --steps "$steps" -o "data/generated/rag_${name}.log"
    echo "data/generated/rag_${name}.log: $(wc -l < "data/generated/rag_${name}.log") lines"
done
