# 使用官方 Python 3.11 slim 映像
FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 更新套件列表
RUN apt-get update

# 安裝基本系統依賴
RUN apt-get install -y \
    wget \
    gnupg \
    ca-certificates

# 安裝字體（嘗試安裝，失敗也繼續）
RUN apt-get install -y fonts-liberation fonts-dejavu-core || true

# 安裝 Playwright 必需的系統庫
RUN apt-get install -y \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libdrm2 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libxss1 \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# 複製 requirements.txt 並安裝 Python 依賴
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 安裝 Playwright 瀏覽器
RUN playwright install chromium

# 跳過有問題的依賴安裝命令
# RUN playwright install-deps chromium

# 複製應用程式代碼
COPY . .

# 設定環境變數
ENV PYTHONPATH=/app
ENV PORT=8080

# 暴露端口
EXPOSE 8080

# 啟動命令
CMD ["python", "app.py"]
