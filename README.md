# Skrejper

Skrejpanje oglasa za posao i izvlačenje kontakata poslodavaca s tri izvora:

| Izvor | Što je | Treba li preglednik |
|---|---|---|
| **HZZ** — `burzarada.hzz.hr` | hrvatski oglasi, 20 kategorija s podkategorijama | da (Chromium) |
| **Arbeitsagentur** — `rest.arbeitsagentur.de` | njemački javni Jobsuche API, 15 grupa Berufsfelder | ne, čisti HTTP |
| **GVP** — `personaldienstleister.de` | imenik članova njemačkog udruženja agencija za zapošljavanje, ~8.700 unosa (centrale i podružnice) | ne, čisti HTTP |

Postoje dva načina korištenja: **desktop aplikacija** (za svakodnevni rad) i **skripte / agent**
(za server i cron).

---

## Desktop aplikacija

Prozorska aplikacija za macOS i Windows. Bez servera, bez baze, bez API ključa — sve ostaje
na tvom računalu.

### Instalacija

Skini zip za svoju platformu iz [Releases](../../releases) ili iz artifacta zadnjeg
*Desktop build* workflowa, raspakiraj i pokreni.

- **macOS** — prevuci `Skrejper.app` u Applications. To je cijela instalacija.
- **Windows** — aplikacija je **portable**, nema instalera. Raspakiraj mapu negdje gdje
  ostaje (npr. `C:\Users\<ime>\Apps\Skrejper`) i napravi prečac: desni klik na
  `Skrejper.exe` → *Show more options* → *Send to* → *Desktop (create shortcut)*.

  Nema `.msi` namjerno: sve što aplikacija pamti — stanje „već skrejpano" i preuzeti
  Chromium — živi u `%LOCALAPPDATA%\Skrejper`, izvan mape aplikacije. Update je zato
  samo brisanje stare mape i raspakiravanje nove; ništa se ne gubi. Isto tako, build je
  `onedir`, a ne `onefile`, jer bi Playwright inače pri svakom pokretanju iznova
  raspakiravao svoj Node driver i aplikacija bi se otvarala osjetno sporije.

### Windows Defender javi „Trojan:Script/Wacatac.B!ml"

Lažna uzbuna, i česta za PyInstaller aplikacije. Sufiks **`!ml`** znači da je detekcija iz
strojnog modela — heuristika, ne potpis stvarnog zloćudnog koda. Okidači su nepotpisan `.exe`
bez reputacije (svaki novi build je „prvi put viđen") i to što zip nosi i Python i cijeli
Node.js runtime koji Playwright treba, uz stotine `.js` datoteka.

Dvije stvari koje najviše izazivaju ovakve detekcije već izbjegavamo: build je `onedir`
(ne `onefile`) i bez UPX kompresije.

Što napraviti:

1. **Vrati datoteku** — Windows Security → *Protection history* → ta stavka → *Actions* →
   *Allow on device*, pa ponovno raspakiraj.
2. **Dodaj iznimku** za mapu u koju raspakiraš (`Virus & threat protection` → *Manage settings*
   → *Exclusions*). Dovoljna je ta jedna mapa — nemoj gasiti zaštitu.
3. **Prijavi lažnu detekciju** Microsoftu na
   [microsoft.com/wdsi/filesubmission](https://www.microsoft.com/en-us/wdsi/filesubmission).
   Besplatno je, obično se riješi u dan-dva, i popravi za sve koji skinu isti build.

Trajno rješenje je **potpisivanje koda** (OV/EV certifikat, reda 200–400 €/god). Dok build nije
potpisan, ovo se može ponoviti nakon svake nove verzije jer svaki build kreće bez reputacije.

**Buildovi nisu potpisani**, pa ih OS prvi put blokira:

- **macOS** — desni klik na `Skrejper.app` → *Open* → *Open*. Ako se i dalje buni:
  ```
  xattr -dr com.apple.quarantine /Applications/Skrejper.app
  ```
- **Windows** — SmartScreen → *More info* → *Run anyway*.

Za HZZ se pri prvom pokretanju jednom preuzima Chromium (~150 MB) u mapu aplikacije.
Arbeitsagentur radi odmah.

### Kako radi

Odabereš kategorije, klikneš **Pokreni skrejpanje** i gledaš kako redovi ulaze uživo.
Rezultati idu u `~/Documents/Skrejper` (može se promijeniti), po jedna datoteka po kategoriji:

```
hzz-hospitality_tourism-2026-08-04.csv
hzz-hospitality_tourism-2026-08-04.xlsx
arbeitsagentur-bau_ausbau-2026-08-04.csv
arbeitsagentur-bau_ausbau-2026-08-04.xlsx
```

CSV je u istom formatu kao onaj iz skripti (`utf-8-sig`, svi navodnici), pa se datoteke iz
aplikacije i sa servera mogu miješati bez ikakvog sređivanja. XLSX ima iste kolone,
zamrznuto zaglavlje i filtere.

Korisno znati:

- **„Preporučene”** na HZZ tabu odabire ona zanimanja koja se skrejpaju u produkciji
  (ista lista kao `scripts/run_hzz_*.py`).
- **„Preskoči oglase koje sam već skrejpao”** pamti se trajno, pa svaki sljedeći run plaća
  samo nove oglase. Zapamti se **samo oglas iz kojeg je izvučen e-mail** — oglas bez kontakta
  provjerava se ponovno idući put.
- **Zaustavi** stvarno prekida posao. Redovi se pišu u CSV kako pristižu, pa ono što je do
  tada prikupljeno ostaje spremljeno.
- Na Arbeitsagentur tabu *Maks. stranica* je **po Berufsfeldu**, ne po kategoriji — kategorija
  s 19 polja napravi 19 × toliko pretraga.

### GVP članovi

Za razliku od burzi, ovo je **imenik**: cilj je pokupiti *sve* firme, a ne samo one s e-mailom.
Zato GVP tab piše dvije datoteke s istim kolonama:

```
gvp-mitglieder-2026-09-06.csv                  firme s e-mailom (kao i ostali izvori)
gvp-mitglieder-2026-09-06-missing-emails.csv   firme bez e-maila — worklist za obogaćivanje
```

U imeniku sâm e-mail ima manjina članova, ali gotovo svi imaju web. Zato je uključena opcija
**„Potraži e-mail na webu firme”**: za svakog člana bez e-maila otvori se njegova stranica te
Impressum/Kontakt (najviše *Maks. stranica po webu* zahtjeva) i uzme prva adresa koja izgleda
kao firmina — prednost imaju `mailto:` linkovi, adrese na domeni firme i opći sandučići
(`info@`, `kontakt@`…). Kolona `email_source` kaže odakle je adresa: `gvp` (iz imenika) ili
`website`. Prepoznaju se i zamaskirane adrese tipa `info (at) firma (dot) de`.

Korisno znati:

- Sâm imenik (bez traženja po webovima) prođe se za desetak minuta; traženje po webovima
  traje sate, jer je to nekoliko sekundi po firmi. *Zaustavi* u svakom trenutku ostavlja obje
  datoteke spremljene.
- Webovi na kojima e-mail **nije** nađen pamte se (uz „Preskoči firme koje sam već skrejpao”),
  pa idući run ne otvara iste stranice iznova. Sama firma se pritom **ne** pamti — ostaje u
  „missing-emails” listi i provjerava se ponovno ako se u imeniku pojavi adresa.
- Centrala i podružnice su zasebni unosi, adresa je obično samo na jednom. Uz „Samo jedan
  unos po firmi” podružnice bez e-maila otpadaju čim je centrala dala e-mail, a firma bez
  ijedne adrese ulazi u worklist jednom.
- Filtri (firma, mjesto, PLZ, Geschäftsfeld, Qualitätsstandard, Hauptstelle/Niederlassung) su
  isti kao na stranici i ulaze u ime datoteke, pa se dva filtrirana runa istog dana ne
  pregaze.
- Stranica je iza Sucuri firewalla koji blokira po IP adresi (datacentri, VPN-ovi). S kućne ili
  uredske mreže radi; ako javi *„Sucuri firewall, HTTP 403”*, pokušaj s druge mreže.

Sučelje je u Windows 11 (Fluent) stilu i prati sistemsku svijetlu/tamnu temu. Isti izgled je i na
macu — namjerno, da aplikacija izgleda isto na oba računala.

### Pokretanje iz koda

```bash
pip install -r requirements-desktop.txt
python skrejper_gui.py
```

### Build

Buildovi nastaju na GitHub Actionsu (`.github/workflows/desktop-build.yml`) za macOS
(Apple Silicon) i Windows:

- **svaki push** u `main`, `openclaw` ili `claude/**` → dva zipa kao artifact u Actions tabu
  (čuvaju se 30 dana); artifact se šalje s `archive: false`, pa se raspakirava **jednom** —
  bez toga `upload-artifact` zapakira naš zip u još jedan zip
- **tag `v*`** (npr. `git tag v1.0.0 && git push origin v1.0.0`) → isto to, plus GitHub Release

`Skrejper-macOS-arm64` radi samo na Apple Siliconu (M1 i noviji). Ako ikad zatreba i za stariji
Intel Mac, u matricu treba vratiti `macos-15-intel` — `macos-13` je ugašen u prosincu 2025.

Gumb *Run workflow* pojavljuje se tek kad workflow postoji na **default grani** repozitorija —
GitHub ga inače ne prikazuje.

Lokalno:

```bash
pip install -r requirements-desktop.txt pyinstaller pillow
python packaging/make_icons.py --platform
pyinstaller packaging/skrejper.spec --noconfirm
```

`.app` se može napraviti samo na macu, `.exe` samo na Windowsima — zato Actions.

### Gdje aplikacija drži svoje

| | macOS | Windows |
|---|---|---|
| stanje „već viđeno”, preglednik | `~/Library/Application Support/Skrejper` | `%LOCALAPPDATA%\Skrejper` |
| rezultati (zadano) | `~/Documents/Skrejper` | `%USERPROFILE%\Documents\Skrejper` |

Obje putanje se mogu pregaziti kroz `SKREJPER_STATE_DIR` i `PLAYWRIGHT_BROWSERS_PATH`.

---

## Skripte i agent (server)

Nepromijenjeno — desktop aplikacija ništa od ovoga ne dira.

```bash
pip install -r requirements.txt
python -m playwright install chromium

python scripts/run_hzz_hospitality_tourism.py     # HZZ, jedna kategorija
python scripts/run_bau_ausbau.py                  # Arbeitsagentur, jedna kategorija
python run_arbeitsagentur_multi.py                # više kategorija odjednom
python scripts/run_gvp_mitglieder.py              # GVP imenik; GVP_ENRICH=false bez traženja po webovima
```

Skripte na kraju šalju CSV mailom preko Resenda (`scripts/send_report.py`,
traži `RESEND_API_KEY` i `RESEND_TO`). Desktop aplikacija ne šalje ništa.

HTTP agent (`agent/main.py`, FastAPI, zaštićen `x-api-key` headerom):

```bash
docker compose up -d
curl -X POST localhost:8000/scrape/arbeitsagentur \
  -H "x-api-key: $AGENT_API_KEY" -H 'content-type: application/json' \
  -d '{"category":"bau_ausbau","max_pages":5}' -o leads.csv
```

---

## Struktura

```
app/scrapers/       hzz.py, arbeitsagentur.py, gvp.py, meinestadt.py — sami skreperi
app/seen_store.py   trajno „već skrejpano”, po izvoru
app/desktop/        desktop aplikacija (PySide6)
  pipeline.py         dedupe + filtriranje, dijeli se sa skriptama
  runner.py           radni proces: skreper → CSV/XLSX, javlja se NDJSON-om
  process.py          GUI strana: pokreće i prekida radni proces
  tabs/               HZZ, Arbeitsagentur i GVP forme
agent/main.py       HTTP agent
scripts/            CLI runneri po kategoriji
packaging/          PyInstaller spec + generator ikona
tests/              unittest (bez mreže)
```

Testovi:

```bash
QT_QPA_PLATFORM=offscreen python -m unittest discover -s tests -t .
```
