# Deployment Guide (Server)

This guide is for long-running deployment of `TabClaw` on Linux servers.

## 1) Required paths and environment

- Project path: `/root/TabClaw-dev/TabClaw`
- Conda env: `tabclaw`
- Service port: `8018`

Before deploying, make sure `setting.txt` exists in the project root and contains valid values:

```ini
API_KEY="..."
BASE_URL="https://.../v1"
DEFAULT_MODEL=deepseek-ai/DeepSeek-V3
DEFAULT_MODEL_EXTRA_JSON=
```

## 2) Install dependencies (inside conda env)

```bash
cd /root/TabClaw-dev/TabClaw
conda run -n tabclaw pip install -r requirements.txt
```

## 3) Create a systemd service (recommended)

Create `/etc/systemd/system/tabclaw.service`:

```ini
[Unit]
Description=TabClaw Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/TabClaw-dev/TabClaw
ExecStart=/root/anaconda3/bin/conda run -n tabclaw python -m uvicorn app:app --host 0.0.0.0 --port 8018
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
systemctl daemon-reload
systemctl enable --now tabclaw
systemctl status tabclaw --no-pager
```

## 4) Operations

```bash
# restart after code/config update
systemctl restart tabclaw

# stop service
systemctl stop tabclaw

# live logs
journalctl -u tabclaw -f
```

## 5) Verify service health

```bash
ss -ltnp | awk '/:8018 /{print}'
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8018/
```

Expected:

- `ss` shows a Python process listening on `0.0.0.0:8018`
- HTTP status code is `200`
