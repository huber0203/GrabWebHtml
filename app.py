# -*- coding: utf-8 -*-

import asyncio
import logging
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from playwright.async_api import async_playwright, Locator, TimeoutError as PlaywrightTimeoutError
import uvicorn

# --- Logging 設定 ---
# 設定日誌記錄，方便追蹤程式運行狀況
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- Pydantic 模型定義 (用於 API 回應) ---
# 定義 API 回應的資料結構，確保格式一致
class Detail(BaseModel):
    html: str
    fetched: bool
    fetch_time_seconds: float
    retry_count: int
    error: Optional[str] = None

class CompanyInfo(BaseModel):
    index: int
    date: str
    time: str
    code: str
    company: str
    subject: str
    hasDetail: bool
    detail: Optional[Detail] = None

class ScrapeResponse(BaseModel):
    success: bool
    total_records: int
    successful_details: int
    failed_details: int
    total_time_seconds: float
    data: List[CompanyInfo]
    timestamp: datetime

# --- FastAPI 應用程式實例 ---
app = FastAPI(
    title="MOPS 公開資訊觀測站爬蟲 API (修正版)",
    description="採用原子化操作，從根本解決資料錯位問題的非同步 MOPS 爬蟲程式。"
)

# --- 核心爬蟲邏輯 (已重構) ---

async def process_single_row(row_locator: Locator, index: int) -> CompanyInfo:
    """
    【核心修正】處理單一表格列，以原子化操作抓取資料與詳細HTML。
    此函數確保了資料提取和按鈕點擊都在同一 `row` 元素內完成，杜絕了資料錯位的可能。
    """
    base_info = {
        "index": index,
        "date": "N/A", "time": "N/A", "code": "N/A", 
        "company": f"Unknown_{index}", "subject": "N/A", "hasDetail": False
    }

    try:
        # 1. 在 `row_locator` 的範圍內，提取表格中的基本資料
        base_info["date"] = await row_locator.locator('td').nth(0).inner_text()
        base_info["time"] = await row_locator.locator('td').nth(1).inner_text()
        base_info["code"] = await row_locator.locator('td').nth(2).inner_text()
        base_info["company"] = await row_locator.locator('td').nth(3).inner_text()
        base_info["subject"] = (await row_locator.locator('td').nth(4).inner_text()).replace('\r\n', ' ').strip()
        base_info["hasDetail"] = True

        logger.info(f"處理中 (Index {index}): {base_info['code']} {base_info['company']}")

        # 2. 在 `row_locator` 的範圍內，找到並點擊「查看」按鈕
        view_button = row_locator.locator('input[type="button"]:has-text("查看")')
        page = row_locator.page
        context = page.context
        
        # 3. 執行點擊並等待新視窗
        max_retries = 3
        for attempt in range(max_retries):
            detail_start_time = datetime.now()
            try:
                async with context.expect_page(timeout=20000) as page_info:
                    await view_button.click(timeout=15000)
                
                detail_page = await page_info.value
                
                # 在新分頁中等待並抓取內容
                await detail_page.wait_for_load_state("domcontentloaded", timeout=20000)
                detail_html = await detail_page.content()
                await detail_page.close()
                
                fetch_time = (datetime.now() - detail_start_time).total_seconds()
                logger.info(f"  -> Index {index} 成功, 耗時: {fetch_time:.2f}s")

                base_info["detail"] = Detail(
                    html=detail_html,
                    fetched=True,
                    fetch_time_seconds=fetch_time,
                    retry_count=attempt
                )
                return CompanyInfo(**base_info)

            except PlaywrightTimeoutError as e:
                logger.warning(f"  -> Index {index} 第 {attempt + 1}/{max_retries} 次嘗試超時: {str(e)}")
                if attempt == max_retries - 1:
                    raise  # 最後一次重試失敗，拋出異常
                await asyncio.sleep(2) # 等待後重試
            except Exception as e:
                logger.error(f"  -> Index {index} 發生非預期錯誤: {e}")
                base_info["detail"] = Detail(
                    html="", fetched=False, fetch_time_seconds=0, 
                    retry_count=attempt, error=f"Unexpected error: {str(e)}"
                )
                return CompanyInfo(**base_info)

    except Exception as e:
        error_message = f"處理 Index {index} 最終失敗: {str(e)}"
        logger.error(error_message)
        base_info["detail"] = Detail(
            html="", fetched=False, fetch_time_seconds=0, 
            retry_count=max_retries if 'max_retries' in locals() else 0, 
            error=error_message
        )
    
    return CompanyInfo(**base_info)


# --- FastAPI 端點 (Endpoint) ---

@app.get("/", summary="API 根目錄")
async def root():
    """提供 API 的基本資訊"""
    return {
        "message": "歡迎使用修正版的 MOPS 公開資訊觀測站爬蟲 API",
        "usage": "請使用 POST 方法訪問 /scrape-mops 來開始抓取資料"
    }

class MopsRequest(BaseModel):
    max_concurrent: int = Field(5, description="最大同時處理的併發任務數量", ge=1, le=20)
    limit: Optional[int] = Field(None, description="限制處理的資料筆數，用於測試", ge=1)

@app.post("/scrape-mops", response_model=ScrapeResponse, summary="抓取 MOPS 當日重大訊息")
async def scrape_mops_data(req: MopsRequest):
    """
    啟動 Playwright 瀏覽器，前往公開資訊觀測站，
    並以安全的方式逐一抓取每家公司的重大訊息詳細內容。
    """
    start_time = datetime.now()
    logger.info(f"接收到爬取請求 (併發數: {req.max_concurrent}, 限制: {req.limit or '無'})...")
    
    url = "https://mops.twse.com.tw/mops/web/t05sr01_1"
    
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            
            logger.info(f"正在導航至: {url}")
            await page.goto(url, timeout=60000, wait_until='domcontentloaded')
            
            logger.info("頁面載入完成，等待表格元素出現...")
            await page.wait_for_selector('table.hasBorder', timeout=30000)
            
            # 【核心修正】定位所有包含「查看」按鈕的資料列
            rows_with_button = page.locator('table.hasBorder > tbody > tr:has(input[type="button"]:has-text("查看"))')
            row_count = await rows_with_button.count()
            
            # 應用 limit 限制
            process_count = min(row_count, req.limit) if req.limit is not None else row_count
            logger.info(f"找到 {row_count} 筆可處理的資料，將處理其中 {process_count} 筆...")

            # 建立非同步任務列表
            semaphore = asyncio.Semaphore(req.max_concurrent)
            
            async def semaphore_task_wrapper(row_locator, index):
                async with semaphore:
                    return await process_single_row(row_locator, index)
            
            tasks = [
                semaphore_task_wrapper(rows_with_button.nth(i), i) 
                for i in range(process_count)
            ]
            
            # 平行執行所有任務
            results = await asyncio.gather(*tasks)
            
        except PlaywrightTimeoutError as e:
            logger.error(f"導航或尋找表格時發生超時錯誤: {e}")
            raise HTTPException(status_code=504, detail="頁面載入或元素定位超時")
        except Exception as e:
            logger.error(f"爬取過程中發生嚴重錯誤: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            if 'browser' in locals() and browser.is_connected():
                await browser.close()
                logger.info("瀏覽器已關閉。")

    total_time = (datetime.now() - start_time).total_seconds()
    successful_count = sum(1 for r in results if r.detail and r.detail.fetched)
    failed_count = len(results) - successful_count

    logger.info(f"所有資料處理完成！成功: {successful_count}, 失敗: {failed_count}, 總耗時: {total_time:.2f}s")

    return ScrapeResponse(
        success=True,
        total_records=len(results),
        successful_details=successful_count,
        failed_details=failed_count,
        total_time_seconds=total_time,
        data=results,
        timestamp=datetime.now()
    )

# --- 健康檢查 ---
@app.get("/healthz", summary="健康檢查")
def healthz():
    return {"status": "ok", "version": "2.0.0-fixed"}

# --- 程式進入點 ---
if __name__ == "__main__":
    # 使用 uvicorn 啟動 FastAPI 應用程式
    # 在終端機中運行 `python mops_crawler_fixed.py` 即可啟動
    uvicorn.run(app, host="0.0.0.0", port=8000)

