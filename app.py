# -*- coding: utf-8 -*-

import asyncio
import logging
import sys
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, AnyHttpUrl
from playwright.async_api import async_playwright, Locator, TimeoutError as PlaywrightTimeoutError
import uvicorn
from bs4 import BeautifulSoup
import re

# --- Logging 設定 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- Pydantic 模型定義 ---
class Detail(BaseModel):
    html: str
    fetched: bool
    fetch_time_seconds: float
    retry_count: int
    error: Optional[str] = None
    page_type: Optional[str] = None  # 新增：標記是 alert 還是新視窗

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
    page_version: str  # 新增：標記頁面版本

# --- HTML 清理函式 ---
def clean_html_content(html_content: str) -> str:
    """清理 HTML，移除不必要的標籤和屬性"""
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 移除所有 script 和 style 標籤
        for tag in soup.find_all(['script', 'style']):
            tag.decompose()
        
        # 嘗試找到核心內容區塊
        core_content = soup.find('div', id='queryResults') or soup.find('div', class_='content-area')
        if core_content:
            return str(core_content)
        else:
            body_content = soup.find('body')
            return str(body_content) if body_content else html_content
            
    except Exception as e:
        logger.error(f"HTML 清理失敗: {e}")
        return html_content

# --- FastAPI 應用程式 ---
app = FastAPI(
    title="MOPS 通用爬蟲 API",
    description="支援新舊版 MOPS 頁面的智慧爬蟲"
)

# --- 頁面版本偵測 ---
async def detect_page_version(page) -> str:
    """偵測是新版還是舊版 MOPS 頁面"""
    url = page.url
    
    # 檢查 URL 模式
    if "#/web/" in url:
        return "new"
    elif "/mops/web/" in url:
        return "old"
    
    # 如果 URL 無法判斷，檢查頁面結構
    try:
        # 新版通常有 Vue/React 的 #app 或類似結構
        has_app = await page.locator('#app').count() > 0
        if has_app:
            return "new"
    except:
        pass
    
    return "old"  # 預設為舊版

# --- 處理舊版頁面的單一列 ---
async def process_old_version_row(row_locator: Locator, index: int, clean_html: bool, max_retries: int, only_show_list: bool) -> CompanyInfo:
    """處理舊版 MOPS 頁面的單一表格列"""
    base_info = {
        "index": index,
        "date": "N/A", "time": "N/A", "code": "N/A", 
        "company": f"Unknown_{index}", "subject": "N/A", "hasDetail": False
    }

    try:
        # 提取基本資料
        cells = row_locator.locator('td')
        base_info["date"] = await cells.nth(0).inner_text()
        base_info["time"] = await cells.nth(1).inner_text()
        base_info["code"] = await cells.nth(2).inner_text()
        base_info["company"] = await cells.nth(3).inner_text()
        base_info["subject"] = (await cells.nth(4).inner_text()).replace('\r\n', ' ').strip()
        base_info["hasDetail"] = True

        # 如果只顯示列表，則直接返回
        if only_show_list:
            logger.info(f"僅顯示列表 (Index {index}): {base_info['code']} {base_info['company']}")
            return CompanyInfo(**base_info)

        logger.info(f"處理中 (Index {index}): {base_info['code']} {base_info['company']}")

        # 找到並點擊「查看」按鈕
        view_button = row_locator.locator('input[type="button"][value*="查看"], button:has-text("查看")')
        page = row_locator.page
        context = page.context
        
        for attempt in range(max_retries):
            detail_start_time = datetime.now()
            try:
                # 嘗試捕獲新視窗
                async with context.expect_page(timeout=15000) as page_info:
                    await view_button.click(timeout=10000)
                
                detail_page = await page_info.value
                await detail_page.wait_for_load_state("domcontentloaded", timeout=20000)
                detail_html = await detail_page.content()
                await detail_page.close()
                
                fetch_time = (datetime.now() - detail_start_time).total_seconds()
                
                if clean_html:
                    detail_html = clean_html_content(detail_html)
                
                logger.info(f"  -> Index {index} 成功 (新視窗), 耗時: {fetch_time:.2f}s")
                
                base_info["detail"] = Detail(
                    html=detail_html,
                    fetched=True,
                    fetch_time_seconds=fetch_time,
                    retry_count=attempt,
                    page_type="new_window"
                )
                return CompanyInfo(**base_info)

            except PlaywrightTimeoutError:
                # 可能是 alert 而非新視窗
                try:
                    await page.wait_for_timeout(1000) # 短暫等待
                    page.on("dialog", lambda dialog: dialog.accept())
                    
                    logger.info(f"  -> Index {index} 可能是 alert 類型")
                    base_info["detail"] = Detail(
                        html="",
                        fetched=True,
                        fetch_time_seconds=(datetime.now() - detail_start_time).total_seconds(),
                        retry_count=attempt,
                        page_type="alert"
                    )
                    return CompanyInfo(**base_info)
                    
                except Exception:
                    logger.warning(f"  -> Index {index} 處理 alert 失敗 (嘗試 {attempt + 1}/{max_retries})")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(2)
                    
            except Exception as e:
                logger.error(f"  -> Index {index} 點擊或讀取詳細資料時發生錯誤 (嘗試 {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    base_info["detail"] = Detail(
                        html="", fetched=False, fetch_time_seconds=0, 
                        retry_count=attempt, error=str(e)
                    )
                    return CompanyInfo(**base_info)
                await asyncio.sleep(2) # 重試前等待

    except Exception as e:
        logger.error(f"處理 Index {index} 提取基本資料時失敗: {e}")
        base_info["detail"] = Detail(
            html="", fetched=False, fetch_time_seconds=0, 
            retry_count=0, error=str(e)
        )
    
    return CompanyInfo(**base_info)

# --- 處理新版頁面的單一列 ---
async def process_new_version_row(row_locator: Locator, index: int, clean_html: bool, max_retries: int, only_show_list: bool) -> CompanyInfo:
    """處理新版 MOPS 頁面的單一表格列"""
    base_info = {
        "index": index,
        "date": "N/A", "time": "N/A", "code": "N/A", 
        "company": f"Unknown_{index}", "subject": "N/A", "hasDetail": False
    }

    try:
        # 新版頁面可能使用不同的標籤結構
        cells = row_locator.locator('td, .cell, .table-cell')
        cell_count = await cells.count()
        
        if cell_count >= 5:
            base_info["date"] = await cells.nth(0).inner_text()
            base_info["time"] = await cells.nth(1).inner_text()
            base_info["code"] = await cells.nth(2).inner_text()
            base_info["company"] = await cells.nth(3).inner_text()
            base_info["subject"] = (await cells.nth(4).inner_text()).replace('\r\n', ' ').strip()
            base_info["hasDetail"] = True

        # 如果只顯示列表，則直接返回
        if only_show_list:
            logger.info(f"僅顯示列表 (Index {index}): {base_info['code']} {base_info['company']}")
            return CompanyInfo(**base_info)

        logger.info(f"處理中 (Index {index}): {base_info['code']} {base_info['company']}")

        view_button = row_locator.locator('button:has-text("查看"), input[value*="查看"], a:has-text("查看")')
        
        if await view_button.count() > 0:
            page = row_locator.page
            context = page.context
            
            for attempt in range(max_retries):
                detail_start_time = datetime.now()
                try:
                    async with context.expect_page(timeout=15000) as page_info:
                        await view_button.click(timeout=10000)
                    
                    detail_page = await page_info.value
                    await detail_page.wait_for_load_state("networkidle", timeout=30000)
                    detail_html = await detail_page.content()
                    await detail_page.close()
                    
                    fetch_time = (datetime.now() - detail_start_time).total_seconds()
                    
                    if clean_html:
                        detail_html = clean_html_content(detail_html)
                    
                    logger.info(f"  -> Index {index} 成功, 耗時: {fetch_time:.2f}s")
                    
                    base_info["detail"] = Detail(
                        html=detail_html,
                        fetched=True,
                        fetch_time_seconds=fetch_time,
                        retry_count=attempt,
                        page_type="new_window"
                    )
                    return CompanyInfo(**base_info)
                    
                except Exception as e:
                    logger.warning(f"  -> Index {index} 詳細資料取得失敗 (嘗試 {attempt + 1}/{max_retries}): {e}")
                    if attempt == max_retries - 1:
                        base_info["detail"] = Detail(
                            html="", fetched=False, fetch_time_seconds=0, 
                            retry_count=attempt, error=str(e)
                        )
                    else:
                        await asyncio.sleep(2)
        else:
            logger.warning(f"  -> Index {index} 找不到 '查看' 按鈕")
            base_info["hasDetail"] = False

    except Exception as e:
        logger.error(f"處理 Index {index} 提取基本資料時失敗: {e}")
        base_info["detail"] = Detail(
            html="", fetched=False, fetch_time_seconds=0, 
            retry_count=0, error=str(e)
        )
    
    return CompanyInfo(**base_info)

# --- 主要 API 端點 ---
class MopsRequest(BaseModel):
    url: AnyHttpUrl = Field(
        "https://mops.twse.com.tw/mops/web/t05sr01_1", 
        description="要抓取的 MOPS 頁面 URL"
    )
    max_concurrent: int = Field(5, description="最大併發數", ge=1, le=20)
    limit: Optional[int] = Field(None, description="限制處理筆數", ge=1)
    clean_html: bool = Field(False, description="是否清理 HTML")
    wait_time: int = Field(5, description="新版頁面額外等待時間（秒）", ge=0, le=30)
    max_retries: int = Field(3, description="失敗重試次數", ge=1, le=10)
    
    # --- [修正] 新增缺少的欄位 ---
    only_scrap_indices: Optional[Any] = Field(
        None, 
        description="指定只抓取特定索引的資料。可以是索引列表 [0, 2, 5] 或 'ALL'。預設為 None (處理全部)。"
    )
    only_show_list: bool = Field(
        False, 
        description="若為 True，則只抓取列表資訊，不點擊「查看」獲取詳細內容。"
    )
    batch_delay: float = Field(
        0, 
        description="每批次處理完成後的延遲秒數。設為 0 以禁用。", 
        ge=0
    )
    # --- 修正結束 ---

@app.post("/scrape-mops", response_model=ScrapeResponse)
async def scrape_mops_data(req: MopsRequest):
    """智慧抓取 MOPS 資料，自動偵測並處理新舊版頁面"""
    start_time = datetime.now()
    logger.info(f"開始爬取: {req.url}")
    logger.info(f"設定: 併發={req.max_concurrent}, 限制={req.limit}, 清理HTML={req.clean_html}")
    
    async with async_playwright() as p:
        browser = None
        try:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            page = await context.new_page()
            
            logger.info(f"導航至: {req.url}")
            await page.goto(str(req.url), timeout=60000, wait_until='domcontentloaded')
            
            page_version = await detect_page_version(page)
            logger.info(f"偵測到頁面版本: {page_version}")
            
            if page_version == "new":
                logger.info(f"新版頁面，額外等待 {req.wait_time} 秒...")
                await page.wait_for_timeout(req.wait_time * 1000)
                
                possible_selectors = [
                    'table tbody tr:has(button:has-text("查看"))',
                    'table tbody tr:has(button)',
                    'div.table-container tr:has(button)',
                    '.data-table tbody tr',
                    'table tr:has(td)',
                    '[role="table"] [role="row"]:has(button)',
                    'tbody tr:has(td:nth-child(5))'
                ]
                
                rows_locator = None
                for selector in possible_selectors:
                    try:
                        temp_locator = page.locator(selector)
                        if await temp_locator.count() > 0:
                            rows_locator = temp_locator
                            logger.info(f"找到資料使用選擇器: {selector}")
                            break
                    except:
                        continue
                
                if not rows_locator:
                    logger.warning("無法找到資料列，嘗試通用選擇器...")
                    rows_locator = page.locator('tr:has(td)')
                    
            else: # old version
                if "t05sr01_1" in str(req.url):
                    logger.info("當日資料頁面，點擊查詢...")
                    try:
                        async with page.expect_response("**/ajax_t05sr01_1", timeout=60000) as response_info:
                            await page.locator('input[type="button"][value*="查詢"]').click(timeout=15000)
                        response = await response_info.value
                        logger.info(f"資料載入完成，狀態: {response.status}")
                    except Exception as e:
                        logger.warning(f"查詢按鈕點擊失敗或超時，繼續處理... ({e})")
                
                await page.locator('table').first.wait_for(timeout=30000)
                rows_locator = page.locator('table.hasBorder > tbody > tr:has(input[value*="查看"]), table.hasBorder > tbody > tr:has(button:has-text("查看"))')
            
            row_count = await rows_locator.count()
            
            process_indices = []
            if req.only_scrap_indices is not None:
                if isinstance(req.only_scrap_indices, list):
                    valid_indices = [i for i in req.only_scrap_indices if 0 <= i < row_count]
                    logger.info(f"只處理指定索引: {valid_indices}")
                    process_indices = valid_indices
                elif isinstance(req.only_scrap_indices, str) and req.only_scrap_indices.upper() == "ALL":
                    logger.info("處理全部資料 (only_scrap_indices='ALL')")
                    process_count = min(row_count, req.limit) if req.limit else row_count
                    process_indices = list(range(process_count))
                # 兼容布林值 False 或字串 "false"
                elif str(req.only_scrap_indices).lower() == "false":
                    logger.info("處理全部資料 (only_scrap_indices=False)")
                    process_count = min(row_count, req.limit) if req.limit else row_count
                    process_indices = list(range(process_count))
                else:
                    logger.warning(f"不支援的 only_scrap_indices 值: {req.only_scrap_indices}，將處理全部資料")
                    process_count = min(row_count, req.limit) if req.limit else row_count
                    process_indices = list(range(process_count))
            else:
                process_count = min(row_count, req.limit) if req.limit else row_count
                process_indices = list(range(process_count))
            
            logger.info(f"找到 {row_count} 筆資料，將處理 {len(process_indices)} 筆")
            
            if len(process_indices) == 0:
                return ScrapeResponse(
                    success=True, total_records=row_count, successful_details=0, 
                    failed_details=0, total_time_seconds=(datetime.now() - start_time).total_seconds(), data=[], 
                    timestamp=datetime.now(), page_version=page_version
                )
            
            semaphore = asyncio.Semaphore(req.max_concurrent)
            
            async def process_with_semaphore(row_locator, index):
                async with semaphore:
                    if page_version == "new":
                        return await process_new_version_row(row_locator, index, req.clean_html, req.max_retries, req.only_show_list)
                    else:
                        return await process_old_version_row(row_locator, index, req.clean_html, req.max_retries, req.only_show_list)
            
            logger.info(f"使用設定: max_retries={req.max_retries}, max_concurrent={req.max_concurrent}, batch_delay={req.batch_delay}s, only_show_list={req.only_show_list}")
            
            results = []
            tasks = [
                process_with_semaphore(rows_locator.nth(idx), idx) 
                for idx in process_indices
            ]
            
            if req.batch_delay > 0 and req.max_concurrent > 0:
                # 分批處理
                for i in range(0, len(tasks), req.max_concurrent):
                    batch = tasks[i:i + req.max_concurrent]
                    logger.info(f"開始處理批次 {i // req.max_concurrent + 1}, 數量: {len(batch)}")
                    batch_results = await asyncio.gather(*batch)
                    results.extend(batch_results)
                    if i + req.max_concurrent < len(tasks):
                        logger.info(f"批次完成，等待 {req.batch_delay} 秒後繼續...")
                        await asyncio.sleep(req.batch_delay)
            else:
                # 全部同時處理
                results = await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"爬取失敗: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            if browser:
                await browser.close()
                logger.info("瀏覽器已關閉")
    
    total_time = (datetime.now() - start_time).total_seconds()
    successful = sum(1 for r in results if r.detail and r.detail.fetched)
    failed = len(results) - successful
    
    logger.info(f"完成！成功: {successful}, 失敗: {failed}, 耗時: {total_time:.2f}s")
    
    return ScrapeResponse(
        success=True,
        total_records=len(results),
        successful_details=successful,
        failed_details=failed,
        total_time_seconds=total_time,
        data=results,
        timestamp=datetime.now(),
        page_version=page_version
    )

@app.get("/")
async def root():
    return {
        "message": "MOPS 通用爬蟲 API",
        "version": "3.0.0",
        "endpoints": {
            "/scrape-mops": "POST - 抓取 MOPS 資料",
            "/healthz": "GET - 健康檢查"
        }
    }

@app.get("/healthz")
def healthz():
    return {"status": "ok", "version": "3.0.0"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
