# -*- coding: utf-8 -*-

import asyncio
import logging
import sys
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, AnyHttpUrl
from playwright.async_api import async_playwright, Locator, Page, TimeoutError as PlaywrightTimeoutError
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
    page_type: Optional[str] = None

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
    page_version: str

# --- HTML 清理函式 ---
def clean_html_content(html_content: str) -> str:
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        for tag in soup.find_all(['script', 'style']):
            tag.decompose()
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
    description="支援新舊版 MOPS 頁面的智慧爬蟲，並為純列表抓取提供高速模式。"
)

# --- [新增] 使用 JavaScript 進行高速列表抓取的函式 ---
async def fetch_list_with_js(page: Page, selector: str) -> List[CompanyInfo]:
    """
    使用 page.evaluate 在瀏覽器端一次性執行 JS 來抓取所有列表資料，速度極快。
    """
    logger.info(f"啟用高速模式：使用 JavaScript 一次性讀取選擇器 '{selector}' 的所有表格資料...")
    
    # 這段 JS 會在瀏覽器內部執行
    all_data = await page.evaluate(f"""
        (selector) => {{
            const rows = document.querySelectorAll(selector);
            const data = [];
            rows.forEach((row, index) => {{
                const cells = row.querySelectorAll('td');
                if (cells.length >= 5) {{
                    data.push({{
                        index: index,
                        date: cells[0] ? cells[0].innerText.trim() : 'N/A',
                        time: cells[1] ? cells[1].innerText.trim() : 'N/A',
                        code: cells[2] ? cells[2].innerText.trim() : 'N/A',
                        company: cells[3] ? cells[3].innerText.trim() : 'N/A',
                        subject: cells[4] ? cells[4].innerText.trim().replace(/\\r\\n|\\n/g, ' ') : 'N/A',
                        hasDetail: row.querySelector('button, input[type="button"]') !== null,
                        detail: null
                    }});
                }}
            }});
            return data;
        }}
    """, selector)
    
    logger.info(f"高速模式完成：成功從瀏覽器獲取 {len(all_data)} 筆資料。")
    return [CompanyInfo(**item) for item in all_data]

# --- 頁面版本偵測 ---
async def detect_page_version(page: Page) -> str:
    url = page.url
    if "#/web/" in url:
        return "new"
    elif "/mops/web/" in url:
        return "old"
    try:
        if await page.locator('#app').count() > 0:
            return "new"
    except:
        pass
    return "old"

# --- 處理單一列的詳細資料（精細模式） ---
async def process_row_detail(row_locator: Locator, index: int, clean_html: bool, max_retries: int, page_version: str) -> CompanyInfo:
    """
    處理單一列的資料提取與「查看」按鈕點擊，僅在精細模式 (only_show_list=False) 下使用。
    """
    base_info = {"index": index, "date": "N/A", "time": "N/A", "code": "N/A", "company": f"Unknown_{index}", "subject": "N/A", "hasDetail": False}
    try:
        cells = row_locator.locator('td, .cell, .table-cell')
        if await cells.count() >= 5:
            base_info["date"] = await cells.nth(0).inner_text()
            base_info["time"] = await cells.nth(1).inner_text()
            base_info["code"] = await cells.nth(2).inner_text()
            base_info["company"] = await cells.nth(3).inner_text()
            base_info["subject"] = (await cells.nth(4).inner_text()).replace('\r\n', ' ').strip()

        logger.info(f"處理中 (Index {index}): {base_info['code']} {base_info['company']}")

        view_button = row_locator.locator('button:has-text("查看"), input[type="button"][value*="查看"]')
        if await view_button.count() == 0:
            base_info["hasDetail"] = False
            return CompanyInfo(**base_info)
        
        base_info["hasDetail"] = True
        page = row_locator.page
        context = page.context

        for attempt in range(max_retries):
            detail_start_time = datetime.now()
            try:
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
                base_info["detail"] = Detail(html=detail_html, fetched=True, fetch_time_seconds=fetch_time, retry_count=attempt, page_type="new_window")
                return CompanyInfo(**base_info)

            except PlaywrightTimeoutError:
                if page_version == "old":
                    logger.info(f"  -> Index {index} 可能是 alert 類型，正在處理...")
                    base_info["detail"] = Detail(html="", fetched=True, fetch_time_seconds=(datetime.now() - detail_start_time).total_seconds(), retry_count=attempt, page_type="alert")
                    return CompanyInfo(**base_info)
                else:
                    logger.warning(f"  -> Index {index} 捕獲新視窗超時 (嘗試 {attempt + 1}/{max_retries})")
                    if attempt == max_retries - 1: raise
            
            except Exception as e:
                logger.error(f"  -> Index {index} 處理失敗 (嘗試 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    raise

    except Exception as e:
        logger.error(f"處理 Index {index} 失敗: {e}")
        base_info["detail"] = Detail(html="", fetched=False, fetch_time_seconds=0, retry_count=max_retries, error=str(e))
    
    return CompanyInfo(**base_info)

# --- 主要 API 端點 ---
class MopsRequest(BaseModel):
    url: AnyHttpUrl = Field("https://mops.twse.com.tw/mops/web/t05sr01_1", description="要抓取的 MOPS 頁面 URL")
    max_concurrent: int = Field(5, description="最大併發數（僅在抓取詳細內容時生效）", ge=1, le=20)
    limit: Optional[int] = Field(None, description="限制處理筆數", ge=1)
    clean_html: bool = Field(False, description="是否清理詳細內容的 HTML")
    wait_time: int = Field(5, description="新版頁面額外等待時間（秒）", ge=0, le=30)
    max_retries: int = Field(3, description="單筆資料失敗重試次數", ge=1, le=10)
    only_show_list: bool = Field(False, description="若為 True，啟用高速模式，僅抓取列表資訊，不點擊「查看」。")
    only_scrap_indices: Optional[List[int]] = Field(None, description="指定只抓取特定索引的資料。例如 [0, 2, 5]。")

@app.post("/scrape-mops", response_model=ScrapeResponse)
async def scrape_mops_data(req: MopsRequest):
    """
    智慧抓取 MOPS 資料。
    - 若 only_show_list=True，則啟用高速模式，瞬間抓取列表。
    - 若 only_show_list=False，則啟用精細模式，併發抓取詳細內容。
    """
    start_time = datetime.now()
    logger.info(f"開始爬取: {req.url}")
    logger.info(f"設定: 併發={req.max_concurrent}, 限制={req.limit}, 清理HTML={req.clean_html}, 僅顯示列表={req.only_show_list}")
    
    async with async_playwright() as p:
        browser = None
        try:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"])
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
            page = await context.new_page()
            
            await page.goto(str(req.url), timeout=60000, wait_until='domcontentloaded')
            
            page_version = await detect_page_version(page)
            logger.info(f"偵測到頁面版本: {page_version}")
            
            selector_string = ""
            if page_version == "new":
                logger.info(f"新版頁面，額外等待 {req.wait_time} 秒...")
                await page.wait_for_timeout(req.wait_time * 1000)
                possible_selectors = ['table tbody tr', 'div.v-table-body tbody tr', '.data-table tbody tr']
                for selector in possible_selectors:
                    if await page.locator(selector).count() > 0:
                        selector_string = selector
                        logger.info(f"找到資料使用選擇器: {selector}")
                        break
            else: # old version
                if "t05sr01_1" in str(req.url):
                    logger.info("舊版當日資料頁面，點擊查詢...")
                    try:
                        await page.locator('input[type="button"][value*="查詢"]').click(timeout=15000)
                        await page.wait_for_load_state("networkidle", timeout=30000)
                    except Exception as e:
                        logger.warning(f"查詢按鈕點擊失敗或超時，繼續處理... ({e})")
                selector_string = 'table.hasBorder > tbody > tr'
            
            if not selector_string:
                raise ValueError("在頁面上找不到任何可識別的資料表格列。")

            total_records_on_page = await page.locator(selector_string).count()
            logger.info(f"在頁面上共找到 {total_records_on_page} 筆資料")

            results = []
            if req.only_show_list:
                # --- [優化] 高速模式 ---
                results = await fetch_list_with_js(page, selector_string)
                if req.only_scrap_indices:
                    results = [res for res in results if res.index in req.only_scrap_indices]
                if req.limit:
                    results = results[:req.limit]
            else:
                # --- [原有] 精細模式 ---
                rows_locator = page.locator(selector_string)
                process_indices = list(range(total_records_on_page))
                if req.only_scrap_indices:
                    process_indices = [i for i in req.only_scrap_indices if i < total_records_on_page]
                if req.limit:
                    process_indices = process_indices[:req.limit]
                
                logger.info(f"將處理 {len(process_indices)} 筆資料的詳細內容...")
                if not process_indices:
                    logger.info("沒有要處理的資料，任務結束。")
                else:
                    semaphore = asyncio.Semaphore(req.max_concurrent)
                    async def process_with_semaphore(index):
                        async with semaphore:
                            return await process_row_detail(rows_locator.nth(index), index, req.clean_html, req.max_retries, page_version)
                    
                    tasks = [process_with_semaphore(i) for i in process_indices]
                    results = await asyncio.gather(*tasks)

        except Exception as e:
            logger.error(f"爬取過程中發生嚴重錯誤: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            if browser:
                await browser.close()
                logger.info("瀏覽器已關閉")
    
    total_time = (datetime.now() - start_time).total_seconds()
    successful = sum(1 for r in results if not req.only_show_list and r.detail and r.detail.fetched)
    failed = len(results) - successful if not req.only_show_list else 0
    
    logger.info(f"任務完成！總耗時: {total_time:.2f}s")
    if not req.only_show_list:
         logger.info(f"詳細資料抓取結果 -> 成功: {successful}, 失敗: {failed}")

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
    return {"message": "歡迎使用 MOPS 通用爬蟲 API", "version": "4.0.0-beta"}

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
