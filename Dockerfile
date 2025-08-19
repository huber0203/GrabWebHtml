# 使用 Playwright 官方基礎映像（最穩定的解決方案）
FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

# 設定工作目錄
WORKDIR /app

# 複製 requirements.txt 並安裝 Python 依賴
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用程式代碼
COPY . .

# 設定環境變數
ENV PYTHONPATH=/app
ENV PORT=8080

# 暴露端口
EXPOSE 8080

# 啟動命令
CMD ["python", "app.py"]
