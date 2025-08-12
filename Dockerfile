# 使用官方 Python 3.11 slim 映像
FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 安裝系統依賴（Playwright 需要）
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 複製 requirements.txt 並安裝 Python 依賴
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 安裝 Playwright 瀏覽器
RUN playwright install chromium
RUN playwright install-deps chromium

# 複製應用程式代碼
COPY . .

# 設定環境變數
ENV PYTHONPATH=/app
ENV PORT=8080

# 暴露端口
EXPOSE 8080

# 啟動命令
CMD ["python", "app.py"]
