#!/usr/bin/env sh
set -eu

PROJECT_DIR="${PROJECT_DIR:-/opt/skrejper}"

cd "$PROJECT_DIR"

docker compose -f docker-compose.prod.yml --profile tls run --rm certbot renew
docker compose -f docker-compose.prod.yml --profile tls exec -T nginx nginx -s reload
