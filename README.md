# EPS Slack Bot

Slack bot trả lời câu hỏi về dữ liệu bảo hiểm (Health & P&C) trên BigQuery bằng Claude.

## Chạy bot

```bash
source venv/bin/activate
python3 app.py
```

Dừng bot: `Ctrl + C`

---

## Re-authentication (khi gặp lỗi auth)

### Lỗi thường gặp & cách fix

#### 1. `Permission 'iam.serviceAccounts.getAccessToken' denied`

ADC token hết hạn hoặc bị revoke. Login lại:

```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project eps-470914
```

> Đăng nhập bằng email `bao.vo@excelplannings.com` (email được grant Service Account Token Creator trên SA `eps-slack-bot`).

---

#### 2. `IAM Service Account Credentials API has not been used... or it is disabled`

Enable API:

```bash
gcloud services enable iamcredentials.googleapis.com --project=eps-470914
```

Đợi ~1 phút rồi thử lại.

---

#### 3. `Permission denied while getting Drive credentials`

Bảng BQ là external table đọc từ Google Sheet. Cần:

1. Đảm bảo SA `eps-slack-bot@eps-470914.iam.gserviceaccount.com` đã được share quyền Viewer trên Google Sheet nguồn của bảng `health_mart` và `pc_mart`.
2. Code đã dùng impersonation với Drive scope — không cần làm gì thêm.

---

#### 4. `bigquery.jobs.create permission denied`

SA thiếu role. Grant:

```bash
gcloud projects add-iam-policy-binding eps-470914 \
  --member="serviceAccount:eps-slack-bot@eps-470914.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding eps-470914 \
  --member="serviceAccount:eps-slack-bot@eps-470914.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
```

---

#### 5. SSL Certificate error (`CERTIFICATE_VERIFY_FAILED`)

```bash
export SSL_CERT_FILE=$(python3 -c "import certifi; print(certifi.where())")
python3 app.py
```

---

## Verify ADC đang dùng email nào

```bash
TOKEN=$(gcloud auth application-default print-access-token)
curl -s "https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=$TOKEN" | python3 -m json.tool
```

Xem trường `email` — phải là `bao.vo@excelplannings.com`.

---

## Slack App

- **Tên app**: Capybara Test
- **Triggers**:
  - Tag `@bot câu hỏi...`
  - React emoji `:capybara:` lên tin nhắn bất kỳ
- **Reinstall app** khi thêm scope mới: https://api.slack.com/apps → chọn app → banner vàng "reinstall" → copy lại `SLACK_BOT_TOKEN` vào `.env`

---

## Required IAM setup (1 lần)

### Service Account
- Tên: `eps-slack-bot@eps-470914.iam.gserviceaccount.com`
- Roles (project-level):
  - BigQuery Job User
  - BigQuery Data Viewer

### User impersonation
- `bao.vo@excelplannings.com` có role **Service Account Token Creator** trên SA `eps-slack-bot`.

### APIs cần enable trên project `eps-470914`
- `iamcredentials.googleapis.com`
- `bigquery.googleapis.com`

### Google Sheets
- SA `eps-slack-bot@...` cần quyền **Viewer** trên các Google Sheet nguồn của external table `health_mart` và `pc_mart`.
