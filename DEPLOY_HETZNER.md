# Deploy na Hetzner Cloud

Ovaj projekt za produkciju treba tri procesa:

- `skrejper`: FastAPI API i sync scrape endpointi
- `celery-worker`: background scrape i enrichment jobovi
- `redis`: queue i Celery backend

`nginx` i `certbot` su opcionalni i služe samo ako TLS terminiraš direktno na istom VM-u.

## 1. Napravi server

Najjednostavnije:

- Hetzner Cloud VM: `CPX21` ili jači
- OS: `Ubuntu 24.04`
- Disk: `40 GB+`
- Firewall:
  - obavezno otvori `22/tcp`
  - za direktni deploy otvori `8000/tcp`
  - za HTTPS deploy otvori `80/tcp` i `443/tcp`

Ako scraper vrtiš češće i paralelno, idi barem na 4 vCPU i 8 GB RAM-a.

## 2. Instaliraj Docker

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
```

Odjavi se i prijavi opet da `docker` radi bez `sudo`.

## 3. Prebaci projekt na server

```bash
sudo mkdir -p /opt/skrejper
sudo chown $USER:$USER /opt/skrejper
git clone <tvoj-repo-url> /opt/skrejper
cd /opt/skrejper
cp .env.example .env
```

U `.env` obavezno postavi:

```env
SUPABASE_URL=...
SUPABASE_SECRET_KEY=...
SCRAPER_API_KEY=...
REDIS_URL=redis://redis:6379/0
CELERY_BROKER_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/0
APP_PORT=8000
CORS_ALLOW_ORIGINS=https://tvoj-frontend.example
```

Ako želiš landing i TLS config po domeni, dodaj i:

```env
APP_DOMAIN=api.example.com
PUBLIC_BASE_URL=https://api.example.com
SERVICE_NAME=Skrejper
```

Ako koristiš `nginx` na istoj VM instanci, stavi:

```env
APP_PORT=127.0.0.1:8000
```

## 4. Pokreni API + scraper worker

Ovo je najbrži deploy bez reverse proxya:

```bash
docker compose -f docker-compose.prod.yml up -d --build skrejper celery-worker redis
```

Provjera:

```bash
docker compose -f docker-compose.prod.yml ps
docker compose -f docker-compose.prod.yml logs -f skrejper
docker compose -f docker-compose.prod.yml logs -f celery-worker
curl http://<server-ip>:8000/health
```

U tom modu API je dostupan direktno na portu `8000`.

## 5. HTTPS na istoj VM instanci

Ako želiš TLS na istoj mašini:

1. Usmjeri DNS `A` record za `APP_DOMAIN` na Hetzner server.
2. U `.env` postavi `APP_DOMAIN` i `PUBLIC_BASE_URL`.
3. Ako želiš da backend ne bude javno otvoren mimo Nginxa, postavi `APP_PORT=127.0.0.1:8000`.
4. Prvi cert izdaješ prije starta `nginx` containera jer `certbot --standalone` mora zauzeti port `80`.

Prvo izdavanje certifikata:

```bash
docker compose -f docker-compose.prod.yml --profile tls run --rm --service-ports certbot \
  certonly --standalone --agree-tos --no-eff-email \
  -m admin@example.com \
  -d api.example.com
```

Zatim digni cijeli stack s TLS profilom:

```bash
docker compose -f docker-compose.prod.yml --profile tls up -d --build
```

Provjera:

```bash
curl https://api.example.com/health
```

## 6. Obnova certifikata

Ručni renew:

```bash
PROJECT_DIR=/opt/skrejper ./scripts/renew-cert.sh
```

Cron jednom dnevno:

```bash
crontab -e
```

Dodaj:

```cron
17 3 * * * PROJECT_DIR=/opt/skrejper /bin/sh /opt/skrejper/scripts/renew-cert.sh >/tmp/skrejper-cert-renew.log 2>&1
```

## 7. Deploy updatea

```bash
cd /opt/skrejper
git pull
docker compose -f docker-compose.prod.yml up -d --build
```

## 8. Što je bitno za ovaj projekt

- `celery-worker` mora biti upaljen jer scrape endpointi po defaultu queueaju posao.
- `redis` mora ostati persistentan, zato je uključen Docker volume `redis_data`.
- `SCRAPER_API_KEY` je obavezan za scrape POST endpointove.
- `SUPABASE_SECRET_KEY` mora biti service-role key, ne anon key.
- Za scraping s Playwrightem ostavljen je `shm_size: 1gb`; ne spuštaj to bez razloga.
