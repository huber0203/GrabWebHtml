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
async def process_old_version_row(row_locator: Locator, index: int, clean_html: bool, max_retries: int = 10) -> CompanyInfo:
    """處理舊版 MOPS 頁面的單一表格列（含重試機制）"""
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

        # 準備標題預覽（限制長度）
        subject_preview = base_info['subject'][:50] + "..." if len(base_info['subject']) > 50 else base_info['subject']
        logger.info(f"處理中 (Index {index}): {base_info['date']} {base_info['time']} {base_info['code']} {base_info['company']} - {subject_preview}")

        # 找到並點擊「查看」按鈕
        view_button = row_locator.locator('input[type="button"][value*="查看"], button:has-text("查看")')
        page = row_locator.page
        context = page.context
        
        max_retries = 3
        for attempt in range(max_retries):
            detail_start_time = datetime.now()
            try:
                if attempt > 0:
                    logger.info(f"  -> Index {index} 重試 {attempt}/{max_retries-1}")
                    await asyncio.sleep(2)  # 重試前等待
                
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
                
                logger.info(f"  -> Index {index} [{base_info['code']} {base_info['company']}] 成功 (新視窗), 耗時: {fetch_time:.2f}s" +
                           (f" (重試 {attempt} 次)" if attempt > 0 else ""))
                
                base_info["detail"] = Detail(
                    html=detail_html,
                    fetched=True,
                    fetch_time_seconds=fetch_time,
                    retry_count=attempt,
                    page_type="new_window"
                )
                return CompanyInfo(**base_info)  # 成功後立即返回

            except PlaywrightTimeoutError:
                # 可能是 alert 而非新視窗
                try:
                    await page.wait_for_timeout(1000)
                    page.on("dialog", lambda dialog: dialog.accept())
                    
                    logger.info(f"  -> Index {index} 是 alert 類型")
                    base_info["detail"] = Detail(
                        html="",
                        fetched=True,
                        fetch_time_seconds=(datetime.now() - detail_start_time).total_seconds(),
                        retry_count=attempt,
                        page_type="alert"
                    )
                    return CompanyInfo(**base_info)
                    
                except Exception:
                    if attempt == max_retries - 1:
                        logger.error(f"  -> Index {index} 最終失敗（超時）")
                        base_info["detail"] = Detail(
                            html="", fetched=False,
                            fetch_time_seconds=(datetime.now() - detail_start_time).total_seconds(),
                            retry_count=attempt, error="Timeout after retries"
                        )
                    
            except Exception as e:
                error_msg = str(e)
                
                # 特殊處理：瀏覽器或頁面已關閉
                if "closed" in error_msg.lower():
                    logger.error(f"  -> Index {index} 瀏覽器已關閉: {error_msg[:100]}")
                    base_info["detail"] = Detail(
                        html="", fetched=False,
                        fetch_time_seconds=(datetime.now() - detail_start_time).total_seconds(),
                        retry_count=attempt, error="Browser closed"
                    )
                    return CompanyInfo(**base_info)  # 不再重試
                
                logger.error(f"  -> Index {index} 發生錯誤: {error_msg[:100]}")
                if attempt == max_retries - 1:
                    base_info["detail"] = Detail(
                        html="", fetched=False,
                        fetch_time_seconds=(datetime.now() - detail_start_time).total_seconds(),
                        retry_count=attempt, error=error_msg
                    )

    except Exception as e:
        logger.error(f"處理 Index {index} 基本資料失敗: {e}")
        base_info["detail"] = Detail(
            html="", fetched=False, fetch_time_seconds=0, 
            retry_count=0, error=str(e)
        )
    
    return CompanyInfo(**base_info)

# --- 處理新版頁面的單一列 ---
async def process_new_version_row(row_locator: Locator, index: int, clean_html: bool, max_retries: int = 10) -> CompanyInfo:
    """處理新版 MOPS 頁面的單一表格列（含重試機制）"""
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
            # 嘗試提取每個欄位的文字內容
            date_text = (await cells.nth(0).inner_text()).strip()
            time_text = (await cells.nth(1).inner_text()).strip()
            code_text = (await cells.nth(2).inner_text()).strip()
            company_text = (await cells.nth(3).inner_text()).strip()
            subject_text = (await cells.nth(4).inner_text()).replace('\r\n', ' ').replace('\n', ' ').strip()
            
            base_info["date"] = date_text if date_text else "N/A"
            base_info["time"] = time_text if time_text else "N/A"
            base_info["code"] = code_text if code_text else "N/A"
            base_info["company"] = company_text if company_text else f"Unknown_{index}"
            base_info["subject"] = subject_text if subject_text else "N/A"
            base_info["hasDetail"] = True
        else:
            # 如果欄位數量不足，嘗試顯示偵錯資訊
            logger.warning(f"Index {index} 只找到 {cell_count} 個欄位，預期至少 5 個")
            # 顯示實際找到的內容
            for i in range(cell_count):
                cell_text = await cells.nth(i).inner_text()
                logger.debug(f"  欄位 {i}: {cell_text[:30]}...")

        # 準備標題預覽（限制長度）
        subject_preview = base_info['subject'][:50] + "..." if len(base_info['subject']) > 50 else base_info['subject']
        logger.info(f"處理中 (Index {index}): {base_info['date']} {base_info['time']} {base_info['code']} {base_info['company']} - {subject_preview}")

        # 新版可能使用 button 而非 input
        view_button = row_locator.locator('button:has-text("查看"), input[value*="查看"], a:has-text("查看")')
        
        if await view_button.count() > 0:
            page = row_locator.page
            context = page.context
            
            max_retries = 3
            for attempt in range(max_retries):
                detail_start_time = datetime.now()
                try:
                    if attempt > 0:
                        logger.info(f"  -> Index {index} 重試 {attempt}/{max_retries-1}")
                        await asyncio.sleep(2)  # 重試前等待
                    
                    # 新版可能使用 SPA 導航或新視窗
                    async with context.expect_page(timeout=15000) as page_info:
                        await view_button.click(timeout=10000)
                    
                    detail_page = await page_info.value
                    await detail_page.wait_for_load_state("networkidle", timeout=30000)
                    detail_html = await detail_page.content()
                    await detail_page.close()
                    
                    fetch_time = (datetime.now() - detail_start_time).total_seconds()
                    
                    if clean_html:
                        detail_html = clean_html_content(detail_html)
                    
                    logger.info(f"  -> Index {index} 成功, 耗時: {fetch_time:.2f}s" + 
                               (f" (重試 {attempt} 次)" if attempt > 0 else ""))
                    
                    base_info["detail"] = Detail(
                        html=detail_html,
                        fetched=True,
                        fetch_time_seconds=fetch_time,
                        retry_count=attempt,
                        page_type="new_window"
                    )
                    return CompanyInfo(**base_info)  # 成功後立即返回
                    
                except PlaywrightTimeoutError as e:
                    logger.warning(f"  -> Index {index} [{base_info['code']} {base_info['company']}] 超時: {str(e)[:100]}")
                    if attempt == max_retries - 1:
                        # 最後一次嘗試失敗
                        base_info["detail"] = Detail(
                            html="", fetched=False, 
                            fetch_time_seconds=(datetime.now() - detail_start_time).total_seconds(), 
                            retry_count=attempt, error=f"Timeout after {max_retries} attempts"
                        )
                        
                except Exception as e:
                    error_msg = str(e)
                    
                    # 特殊處理：瀏覽器或頁面已關閉的錯誤
                    if "closed" in error_msg.lower():
                        logger.error(f"  -> Index {index} [{base_info['code']} {base_info['company']}] 瀏覽器已關閉: {error_msg[:100]}")
                        # 如果還有重試機會，繼續重試
                        if attempt < max_retries - 1:
                            logger.info(f"  -> Index {index} 將在 2 秒後重試...")
                            await asyncio.sleep(2)
                            continue
                        else:
                            # 最後一次重試也失敗了
                            base_info["detail"] = Detail(
                                html="", fetched=False, 
                                fetch_time_seconds=(datetime.now() - detail_start_time).total_seconds(),
                                retry_count=attempt, error="Browser closed after all retries"
                            )
                            return CompanyInfo(**base_info)
                    
                    logger.warning(f"  -> Index {index} [{base_info['code']} {base_info['company']}] 失敗: {error_msg[:100]}")
                    if attempt == max_retries - 1:
                        # 最後一次嘗試失敗
                        base_info["detail"] = Detail(
                            html="", fetched=False,
                            fetch_time_seconds=(datetime.now() - detail_start_time).total_seconds(),
                            retry_count=attempt, error=error_msg
                        )
        else:
            # 沒有找到查看按鈕
            logger.info(f"  -> Index {index} 沒有詳細資料按鈕")
            base_info["detail"] = Detail(
                html="", fetched=False, fetch_time_seconds=0,
                retry_count=0, error="No view button found"
            )

    except Exception as e:
        logger.error(f"處理 Index {index} 基本資料失敗: {e}")
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
    max_retries: int = Field(10, description="失敗重試次數", ge=1, le=20)  # 預設改為 10
    batch_delay: float = Field(0, description="每批次完成後等待時間（秒）", ge=0, le=30)

@app.post("/scrape-mops", response_model=ScrapeResponse)
async def scrape_mops_data(req: MopsRequest):
    """智慧抓取 MOPS 資料，自動偵測並處理新舊版頁面"""
    start_time = datetime.now()
    logger.info(f"開始爬取: {req.url}")
    logger.info(f"設定: 併發={req.max_concurrent}, 限制={req.limit}, 清理HTML={req.clean_html}")
    
    async with async_playwright() as p:
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
            
            # 導航到頁面
            logger.info(f"導航至: {req.url}")
            await page.goto(str(req.url), timeout=60000, wait_until='domcontentloaded')
            
            # 偵測頁面版本
            page_version = await detect_page_version(page)
            logger.info(f"偵測到頁面版本: {page_version}")
            
            # 根據版本執行不同的等待策略
            if page_version == "new":
                # 新版頁面需要更多時間載入
                logger.info(f"新版頁面，額外等待 {req.wait_time} 秒...")
                await page.wait_for_timeout(req.wait_time * 1000)
                
                # 嘗試多種可能的表格選擇器
                possible_selectors = [
                    'table tbody tr:has(button:has-text("查看"))',
                    'table tbody tr:has(button)',  # 更通用的按鈕選擇器
                    'div.table-container tr:has(button)',
                    '.data-table tbody tr',
                    'table tr:has(td)',
                    '[role="table"] [role="row"]:has(button)',
                    'tbody tr:has(td:nth-child(5))'  # 至少有5個欄位的列
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
                    
            else:  # old version
                # 舊版頁面處理
                if "t05sr01_1" in str(req.url):
                    logger.info("當日資料頁面，點擊查詢...")
                    try:
                        async with page.expect_response("**/ajax_t05sr01_1", timeout=60000) as response_info:
                            await page.locator('input[type="button"][value*="查詢"]').click(timeout=15000)
                        response = await response_info.value
                        logger.info(f"資料載入完成，狀態: {response.status}")
                    except:
                        logger.warning("查詢按鈕點擊失敗，繼續處理...")
                
                # 等待表格出現
                await page.locator('table').first.wait_for(timeout=30000)
                rows_locator = page.locator('table.hasBorder > tbody > tr:has(input[value*="查看"])')
            
            # 計算要處理的資料筆數
            row_count = await rows_locator.count()
            process_count = min(row_count, req.limit) if req.limit else row_count
            logger.info(f"找到 {row_count} 筆資料，將處理 {process_count} 筆")
            
            if process_count == 0:
                await browser.close()
                return ScrapeResponse(
                    success=True, total_records=0, successful_details=0, 
                    failed_details=0, total_time_seconds=0, data=[], 
                    timestamp=datetime.now(), page_version=page_version
                )
            
            # 建立併發任務
            semaphore = asyncio.Semaphore(req.max_concurrent)
            
            async def process_with_semaphore(row_locator, index):
                async with semaphore:
                    if page_version == "new":
                        return await process_new_version_row(row_locator, index, req.clean_html, req.max_retries)
                    else:
                        return await process_old_version_row(row_locator, index, req.clean_html, req.max_retries)
            
            # 在建立任務前記錄設定
            logger.info(f"使用設定: max_retries={req.max_retries}, max_concurrent={req.max_concurrent}, batch_delay={req.batch_delay}s")
            
            # 批次處理邏輯
            results = []
            if req.batch_delay > 0:
                # 分批處理，每批 max_concurrent 筆
                for batch_start in range(0, process_count, req.max_concurrent):
                    batch_end = min(batch_start + req.max_concurrent, process_count)
                    batch_size = batch_end - batch_start
                    
                    logger.info(f"開始處理批次: {batch_start//req.max_concurrent + 1}, 資料 {batch_start}-{batch_end-1} (共 {batch_size} 筆)")
                    
                    batch_tasks = [
                        process_with_semaphore(rows_locator.nth(i), i) 
                        for i in range(batch_start, batch_end)
                    ]
                    
                    batch_results = await asyncio.gather(*batch_tasks)
                    results.extend(batch_results)
                    
                    # 如果不是最後一批，則等待
                    if batch_end < process_count:
                        logger.info(f"批次完成，等待 {req.batch_delay} 秒後繼續...")
                        await asyncio.sleep(req.batch_delay)
            else:
                # 原本的邏輯：全部同時處理（受 semaphore 限制）
                tasks = [
                    process_with_semaphore(rows_locator.nth(i), i) 
                    for i in range(process_count)
                ]
                results = await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"爬取失敗: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            if 'browser' in locals():
                await browser.close()
                logger.info("瀏覽器已關閉")
    
    # 統計結果
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
