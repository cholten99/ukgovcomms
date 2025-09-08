# Deploy & Sync (UKGovComms)

## One-time server prep
- Systemd service: `ukgovcomms` (uses `/var/www/ukgovcomms/.venv`).
- Nginx/Gunicorn set up and working.

## CI/CD
- GitHub Actions: `.github/workflows/deploy.yml`
  - Upload code to `/var/www/ukgovcomms`
  - Write `.env` from GitHub Secrets
  - Build venv: `python3 -m venv .venv && pip install -r requirements.txt`
  - Restart: `systemctl restart ukgovcomms`

## DB move (manual)
**Export (home PC):**
```bash
export $(grep -E '^(DB_HOST|DB_NAME|DB_USER|DB_PASSWORD)=' .env | xargs)
mysqldump -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" \
  --single-transaction --quick --routines --events --triggers \
  --set-gtid-purged=OFF --no-tablespaces "$DB_NAME" | gzip > exports/${DB_NAME}_DATE.sql.gz

## Import (server)

export $(grep -E '^(DB_HOST|DB_NAME|DB_USER|DB_PASSWORD)=' .env | xargs)
gunzip -c /var/www/ukgovcomms/imports/FILE.sql.gz | \
  mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME"

## Assets sync (manual)

# From home PC:
rsync -avz assets/sources/ USER@SERVER:/var/www/ukgovcomms/assets/sources/
rsync -avz assets/global/  USER@SERVER:/var/www/ukgovcomms/assets/global/

# On server:
sudo chown -R www-data:www-data /var/www/ukgovcomms/assets

## Quick checks

systemctl status ukgovcomms --no-pager -l
ss -ltnp | grep 8001
curl -I 127.0.0.1:8001/


