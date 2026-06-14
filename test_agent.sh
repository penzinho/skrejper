#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${AGENT_BASE_URL:-http://localhost:8080}"

# Povuci AGENT_API_KEY iz .env.agent ako nije vec u environmentu
if [ -z "${AGENT_API_KEY:-}" ]; then
  if [ -f .env.agent ]; then
    export $(grep -v '^#' .env.agent | grep AGENT_API_KEY | xargs)
  fi
fi

if [ -z "${AGENT_API_KEY:-}" ]; then
  echo "GRESKA: AGENT_API_KEY nije postavljen ni u environmentu ni u .env.agent"
  exit 1
fi

PASS=0
FAIL=0

check() {
  local desc="$1"
  local expected="$2"
  local actual="$3"
  if [ "$actual" = "$expected" ]; then
    echo "  PASS  $desc"
    ((PASS++)) || true
  else
    echo "  FAIL  $desc  (ocekivano=$expected, dobiveno=$actual)"
    ((FAIL++)) || true
  fi
}

echo "Target: $BASE_URL"
echo ""
echo "--- Health ---"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/health")
check "GET /health vraca 200" "200" "$STATUS"

echo ""
echo "--- API key autentikacija ---"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$BASE_URL/scrape/hzz" \
  -H "Content-Type: application/json" \
  -d '{"max_pages":1}')
check "POST /scrape/hzz bez keya vraca 401" "401" "$STATUS"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$BASE_URL/scrape/hzz" \
  -H "x-api-key: pogresankey" \
  -H "Content-Type: application/json" \
  -d '{"max_pages":1}')
check "POST /scrape/hzz s pogresnim keyem vraca 401" "401" "$STATUS"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$BASE_URL/scrape/meinestadt" \
  -H "Content-Type: application/json" \
  -d '{"max_pages":1}')
check "POST /scrape/meinestadt bez keya vraca 401" "401" "$STATUS"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "$BASE_URL/scrape/meinestadt" \
  -H "x-api-key: pogresankey" \
  -H "Content-Type: application/json" \
  -d '{"max_pages":1}')
check "POST /scrape/meinestadt s pogresnim keyem vraca 401" "401" "$STATUS"

echo ""
echo "--- Scrape test (1 podgrupa, moze trajati 30-60 sekundi) ---"

TMPFILE=$(mktemp /tmp/hzz-test-XXXX.csv)
# group=konobari scrapa samo jednu podgrupu — brzo za testiranje
STATUS=$(curl -s --max-time 300 -o "$TMPFILE" -w "%{http_code}" \
  -X POST "$BASE_URL/scrape/hzz" \
  -H "x-api-key: $AGENT_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"category":"hospitality_tourism","group":"Barmeni/barmenice","max_pages":1}')
check "POST /scrape/hzz s ispravnim keyem vraca 200" "200" "$STATUS"

if [ -s "$TMPFILE" ]; then
  ROWS=$(wc -l < "$TMPFILE")
  echo "        CSV redova (ukljucujuci header): $ROWS"
  head -2 "$TMPFILE"
else
  echo "  WARN  CSV je prazan"
fi
rm -f "$TMPFILE"

echo ""
echo "--- Rezultat: $PASS passed, $FAIL failed ---"
[ "$FAIL" -eq 0 ] || exit 1
