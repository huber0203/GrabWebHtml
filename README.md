# HTML Fetcher - Cloud Run 部署

這是一個 FastAPI 應用程式，提供 HTML 抓取服務，支援純 HTTP 抓取和 JavaScript 渲染。

## 功能特色

- `/fetch` - 純 HTTP 抓取，支援各種 HTTP 方法和參數
- `/render` - JavaScript 渲染抓取，使用 Playwright 處理動態內容
- `/healthz` - 健康檢查端點

## 部署到 Google Cloud Run

### 前置準備

1. 確保您有 Google Cloud 專案並啟用以下 API：
   - Cloud Run API
   - Cloud Build API
   - Container Registry API

2. 安裝並設定 Google Cloud CLI

### 手動部署

\`\`\`bash
# 構建並推送映像
gcloud builds submit --tag gcr.io/PROJECT_ID/html-fetcher

# 部署到 Cloud Run
gcloud run deploy html-fetcher \
  --image gcr.io/PROJECT_ID/html-fetcher \
  --region asia-east1 \
  --platform managed \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 300 \
  --concurrency 10 \
  --max-instances 10
\`\`\`

### GitHub 自動部署

1. 在 Google Cloud Console 中設定 Cloud Build GitHub 觸發器
2. 選擇您的 GitHub 儲存庫
3. 設定觸發條件（例如：推送到 main 分支）
4. 使用 `cloudbuild.yaml` 作為構建配置

### 環境變數

應用程式會自動使用 Cloud Run 提供的 `PORT` 環境變數。

### 資源配置

- 記憶體：2GB（Playwright 需要較多記憶體）
- CPU：2 核心
- 超時：300 秒
- 並發：10 個請求
- 最大實例：10 個

## API 使用範例

### 抓取靜態內容
\`\`\`bash
curl -X POST "https://your-service-url/fetch" \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com"}'
\`\`\`

### 渲染動態內容
\`\`\`bash
curl -X POST "https://your-service-url/render" \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com", "wait_until": "networkidle"}'
