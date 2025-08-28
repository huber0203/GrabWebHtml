# -*- coding: utf-8 -*-
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, AnyHttpUrl
from typing import Optional, Dict, Any, Literal
from io import BytesIO
from urllib.parse import urlparse, quote
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from bs4 import BeautifulSoup
import re
import logging
import sys
import asyncio

app = FastAPI(title="Universal HTML Fetcher", version="1.4.1")

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# --------------------
# 共用工具
# --------------------
def build_session(user_agent: Optional[str]) -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)

    sess.headers.update({
        "User-Agent": user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    return sess

def default_filename(url: str, fallback_ext: str = "html") -> str:
    p = urlparse(url)
    base = (p.netloc + p.path).replace("/", "_").strip("_") or "download"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if "." not in base.split("_")[-1]:
        base = f"{base}.{fallback_ext}"
    return f"{base}_{ts}"


def clean_html_content(html_content: str) -> str:
    """
    清理 HTML 內容，移除雜訊，保留重要的表格和文字資訊
    特別針對 goodinfo.tw 股票數據進行優化
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 移除不需要的標籤，但保留可能包含股票數據的標籤
    unwanted_tags = [
        'script', 'style', 'nav', 'header', 'footer', 'aside',
        'iframe', 'embed', 'object', 'applet'
    ]
    
    for tag_name in unwanted_tags:
        for tag in soup.find_all(tag_name):
            tag.decompose()
    
    # 移除廣告相關的元素
    ad_patterns = [
        'gpt-ad', 'google-ad', 'adsense', 'adsbygoogle'
    ]
    
    for pattern in ad_patterns:
        for tag in soup.find_all(attrs={'class': re.compile(pattern, re.I)}):
            tag.decompose()
        for tag in soup.find_all(attrs={'id': re.compile(pattern, re.I)}):
            tag.decompose()
    
    # 移除包含大量 JavaScript 變數定義的文本節點
    for element in soup.find_all(text=True):
        if isinstance(element, str) and len(element) > 200:
            # 檢查是否包含大量程式碼符號，但不要移除可能的股票代碼或數據
            code_indicators = ['const ', 'var ', 'function(', 'googletag', 'window.', 'document.']
            if any(indicator in element for indicator in code_indicators):
                element.replace_with('')
    
    # 移除大部分 JavaScript 事件處理器，但保留一些基本屬性
    for tag in soup.find_all(True):
        # 移除事件處理器
        attrs_to_remove = [attr for attr in tag.attrs if attr.lower().startswith('on')]
        for attr in attrs_to_remove:
            del tag[attr]
        
        # 只移除複雜的 style 屬性，保留基本樣式
        if 'style' in tag.attrs and len(tag['style']) > 100:
            del tag['style']
    
    # 移除表單元素但保留 select 選項（這些包含重要的篩選條件）
    for form_tag in soup.find_all('form'):
        form_tag.decompose()
    
    # 移除複雜的 input 元素
    for input_tag in soup.find_all('input'):
        if input_tag.get('type') not in ['hidden']:
            input_tag.decompose()
    
    # 簡化表格屬性但保留結構
    for table in soup.find_all('table'):
        for tag in table.find_all(True):
            # 保留重要屬性
            keep_attrs = ['class', 'id', 'colspan', 'rowspan', 'bgcolor', 'align', 'valign']
            attrs_to_remove = [attr for attr in tag.attrs if attr not in keep_attrs]
            for attr in attrs_to_remove:
                del tag[attr]
    
    # 只移除真正空的標籤
    for tag in soup.find_all():
        if (not tag.get_text(strip=True) and 
            not tag.find('img') and 
            not tag.find('table') and 
            not tag.find('select') and
            tag.name not in ['br', 'hr', 'tr', 'td', 'th']):
            tag.decompose()
    
    # 清理多餘的空白但保留基本格式
    cleaned_html = str(soup)
    
    # 移除多餘的空行但不要太激進
    cleaned_html = re.sub(r'\n\s*\n\s*\n', '\n\n', cleaned_html)
    
    return cleaned_html

# --------------------
# 請求模型
# --------------------
class FetchRequest(BaseModel):
    url: AnyHttpUrl
    method: Literal["GET", "POST"] = "GET"
    headers: Optional[Dict[str, str]] = None
    params: Optional[Dict[str, Any]] = None
    data: Optional[Any] = None
    referer: Optional[str] = None
    user_agent: Optional[str] = None
    timeout: float = 20.0
    insecure: bool = False
    filename: Optional[str] = None
    pretty_content: bool = False


class RenderRequest(BaseModel):
    url: AnyHttpUrl
    referer: Optional[str] = "https://goodinfo.tw/tw/index.asp"
    user_agent: Optional[str] = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    wait_until: Literal["load", "domcontentloaded", "networkidle"] = "domcontentloaded"
    wait_selector: Optional[str] = Field(
        None, description="等待指定 selector 再抓取，例如 'table'"
    )
    timeout_ms: int = 60000
    disable_js: bool = False
    filename: Optional[str] = None
    viewport: Optional[Dict[str, int]] = None
    cookies: Optional[Dict[str, str]] = None
    pretty_content: bool = False


class FlexibleMopsRequest(BaseModel):
    url: str
    batch_size: int = 10
    max_concurrent: int = 5
    timeout_ms: int = 300000


# --------------------
# /fetch 端點：純 HTTP 抓取
# --------------------
@app.post("/fetch")
def fetch(req: FetchRequest):
    sess = build_session(req.user_agent)
    if req.referer:
        sess.headers["Referer"] = req.referer
    if req.headers:
        hop_by_hop = {"host", "content-length", "transfer-encoding", "connection"}
        for k, v in req.headers.items():
            if k.lower() not in hop_by_hop:
                sess.headers[k] = v
    try:
        resp = sess.request(
            req.method,
            str(req.url),
            params=req.params,
            data=req.data,
            timeout=req.timeout,
            verify=not req.insecure,
            allow_redirects=True,
        )
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {e}")

    content_bytes = resp.content
    origin_ct = resp.headers.get("Content-Type", "application/octet-stream")
    
    # 如果需要清理內容且是 HTML
    if req.pretty_content and "html" in origin_ct.lower():
        try:
            html_content = content_bytes.decode(resp.encoding or 'utf-8')
            cleaned_content = clean_html_content(html_content)
            content_bytes = cleaned_content.encode('utf-8')
            origin_ct = "text/html; charset=utf-8"
        except Exception as e:
            # 如果清理失敗，返回原始內容
            pass
    
    base_name = req.filename or default_filename(str(req.url))
    if ("html" in origin_ct.lower()) and "." not in base_name.split("_")[-1]:
        base_name = f"{base_name}.html"
    
    # 如果是清理過的內容，在檔名中標註
    if req.pretty_content and "html" in origin_ct.lower():
        base_name = base_name.replace('.html', '_cleaned.html')

    ascii_name = base_name.encode("ascii", "ignore").decode("ascii") or "download.html"
    cd = f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{quote(base_name)}'

    return StreamingResponse(
        BytesIO(content_bytes),
        media_type=origin_ct,
        headers={
            "Content-Disposition": cd,
            "X-Origin-Status": str(resp.status_code),
            "X-Origin-URL": str(resp.url),
            **({"X-Origin-Encoding": resp.encoding} if resp.encoding else {}),
            **({"X-Content-Cleaned": "true"} if req.pretty_content else {}),
        },
        status_code=200,
    )


# --------------------
# /render 端點：JS 渲染抓取
# --------------------
@app.post("/render")
async def render(req: RenderRequest):
    async def _once():
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = await browser.new_context(
                user_agent=req.user_agent,
                java_script_enabled=not req.disable_js,
                viewport=req.viewport or {"width": 1366, "height": 900},
                extra_http_headers={
                    "referer": req.referer or "",
                    "accept-language": "zh-TW,zh;q=0.9,en;q=0.8",
                },
            )
            if req.cookies:
                await context.add_cookies(
                    [{"name": k, "value": v, "url": str(req.url)} for k, v in req.cookies.items()]
                )

            page = await context.new_page()
            # 先暖機進入 referer 頁面（可能設 cookie）
            if req.referer:
                try:
                    await page.goto(req.referer, wait_until="domcontentloaded", timeout=min(15000, req.timeout_ms))
                except Exception:
                    pass

            # 進入目標頁
            await page.goto(str(req.url), wait_until=req.wait_until, timeout=req.timeout_ms)
            if req.wait_selector:
                await page.wait_for_selector(req.wait_selector, timeout=req.timeout_ms, state="visible")

            html = await page.content()
            await context.close()
            await browser.close()
            return html

    try:
        try:
            html = await _once()
        except PWTimeout:
            # 二次嘗試
            req.wait_until = "load"
            req.timeout_ms = int(req.timeout_ms * 1.5)
            html = await _once()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Render failed: {e}")

    # 如果需要清理內容
    if req.pretty_content:
        try:
            html = clean_html_content(html)
        except Exception as e:
            # 如果清理失敗，返回原始內容
            pass

    base_name = req.filename or default_filename(str(req.url))
    if not base_name.endswith(".html"):
        base_name += ".html"
    
    # 如果是清理過的內容，在檔名中標註
    if req.pretty_content:
        base_name = base_name.replace('.html', '_cleaned.html')
        
    ascii_name = base_name.encode("ascii", "ignore").decode("ascii") or "page.html"
    cd = f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{quote(base_name)}'

    return StreamingResponse(
        BytesIO(html.encode("utf-8")),
        media_type="text/html; charset=utf-8",
        headers={
            "Content-Disposition": cd,
            **({"X-Content-Cleaned": "true"} if req.pretty_content else {}),
        },
        status_code=200,
    )


# --------------------
# MOPS 安全資料解析函數
# --------------------
async def get_main_data_safely(page):
    """安全地獲取主表格資料，確保沒有 None 元素"""
    
    try:
        main_data = await page.evaluate("""
            () => {
                const dataRows = document.querySelectorAll('table tbody tr');
                const results = [];
                
                Array.from(dataRows).forEach((row, index) => {
                    const cells = Array.from(row.cells);
                    
                    const hasViewButton = row.querySelector('button') && 
                                         row.querySelector('button').textContent.includes('查看');
                    
                    if (cells.length >= 6 && hasViewButton) {
                        // 確保所有字段都有預設值
                        const record = {
                            index: results.length,
                            date: cells[0]?.querySelector('span')?.textContent?.trim() || cells[0]?.textContent?.trim() || '',
                            time: cells[1]?.querySelector('span')?.textContent?.trim() || cells[1]?.textContent?.trim() || '',
                            code: cells[2]?.querySelector('span')?.textContent?.trim() || cells[2]?.textContent?.trim() || '',
                            company: cells[3]?.querySelector('span')?.textContent?.trim() || cells[3]?.textContent?.trim() || `Company_${results.length}`,
                            subject: cells[4]?.querySelector('span')?.textContent?.trim() || cells[4]?.textContent?.trim() || '',
                            hasDetail: hasViewButton,
                            rowElement: index
                        };
                        
                        results.push(record);
                    }
                });
                
                return results;
            }
        """)
    except Exception as e:
        logger.error(f"主資料解析失敗: {e}")
        main_data = []
    
    # 再次檢查並過濾掉任何 None 或無效的記錄
    filtered_data = []
    for i, record in enumerate(main_data or []):
        if record is not None and isinstance(record, dict):
            # 確保必要的字段存在
            if 'company' not in record or not record['company']:
                record['company'] = f'Company_{i}'
            if 'index' not in record:
                record['index'] = i
            filtered_data.append(record)
        else:
            # 創建預設記錄
            filtered_data.append({
                'index': i,
                'date': '',
                'time': '',
                'code': '',
                'company': f'Company_{i}',
                'subject': '',
                'hasDetail': True
            })
    
    logger.info(f"安全解析完成 - 原始: {len(main_data or [])}, 過濾後: {len(filtered_data)}")
    if filtered_data:
        logger.info(f"前3筆資料預覽: {filtered_data[:3]}")
    
    return filtered_data


# --------------------
# MOPS 併發處理輔助函數
# --------------------
async def _process_batch_concurrent(context, buttons, main_data, offset, max_concurrent):
    """併發處理一批按鈕 - 含重試機制"""
    
    # 驗證輸入資料
    if not main_data:
        logger.error("main_data 為空，無法處理批次")
        return []
    
    logger.info(f"批次處理 - 偏移: {offset}, 按鈕數: {len(buttons)}, 主資料數: {len(main_data)}")
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def _process_single_button(button, index):
        """處理單個按鈕 - 含重試機制和 Alert 處理"""
        async with semaphore:
            detail_start = datetime.now()
            global_index = offset + index
            max_retries = 10
            
            for attempt in range(max_retries):
                detail_page = None
                alert_message = None
                dialog_handler = None
                
                try:
                    # 安全獲取 main_data 中的資料
                    current_record = None
                    if global_index < len(main_data) and main_data[global_index] is not None:
                        current_record = main_data[global_index]
                        if not isinstance(current_record, dict):
                            logger.warning(f"記錄 {global_index} 不是字典: {type(current_record)}")
                            current_record = None
                    
                    if current_record is None:
                        # 創建預設記錄
                        current_record = {
                            'index': global_index,
                            'date': '',
                            'time': '',
                            'code': '',
                            'company': f'Unknown_{global_index}',
                            'subject': '',
                            'hasDetail': True
                        }
                        logger.info(f"    [{index+1}] 使用預設記錄 {global_index}")
                    
                    company_name = current_record.get('company', 'N/A')
                    if attempt == 0:
                        logger.info(f"    [{index+1}] 處理 {company_name}")
                    else:
                        logger.info(f"    [{index+1}] 重試 {attempt} - {company_name}")
                    
                    # 設定 alert 監聽器
                    alert_handled = False
                    dialog_processed = False
                    
                    async def handle_dialog(dialog):
                        nonlocal alert_message, alert_handled, dialog_processed
                        if dialog_processed:
                            return
                        
                        dialog_processed = True
                        alert_message = dialog.message or "系統Alert訊息"
                        alert_handled = True
                        logger.warning(f"    [{index+1}] 捕獲 Alert: {alert_message}")
                        
                        try:
                            await dialog.accept()
                        except Exception as e:
                            logger.warning(f"    [{index+1}] Dialog 已被處理: {e}")
                    
                    dialog_handler = handle_dialog
                    context.on("dialog", dialog_handler)
                    
                    try:
                        # 開啟新分頁，但處理可能的 alert
                        async with context.expect_page(timeout=20000) as new_page_info:
                            await button.click()
                            
                            # 短暫等待，看是否有 alert 出現
                            await asyncio.sleep(1.5)
                            
                            if alert_handled:
                                # 如果有 alert，直接返回 alert 資訊
                                logger.info(f"    [{index+1}] Alert 處理完成: {alert_message}")
                                
                                safe_alert_message = alert_message or "系統提示：無法取得詳細資料"
                                
                                return {
                                    **current_record,
                                    "detail": {
                                        "html": "<html><body>Alert</body></html>",
                                        "structured": {
                                            "tables_count": 0,
                                            "page_text": safe_alert_message,
                                            "title": "",
                                            "has_content": True,
                                            "url": "about:blank",
                                            "content_length": len(safe_alert_message)
                                        },
                                        "fetched": True,
                                        "fetch_time_seconds": (datetime.now() - detail_start).total_seconds(),
                                        "retry_count": attempt
                                    }
                                }
                            
                            # 沒有 alert，正常處理新頁面
                            detail_page = await new_page_info.value
                    
                    finally:
                        # 移除 dialog 監聽器
                        if dialog_handler:
                            try:
                                context.remove_listener("dialog", dialog_handler)
                            except:
                                pass
                    
                    # 如果已經處理了 alert，就不繼續了
                    if alert_handled:
                        break
                    
                    # 檢查頁面是否正常
                    if detail_page and detail_page.is_closed():
                        raise Exception("Page was closed immediately after opening")
                    
                    if detail_page:
                        # 等待載入
                        await detail_page.wait_for_load_state("networkidle", timeout=25000)
                        
                        # 取得內容
                        detail_content = await detail_page.content()
                        
                        # 解析結構化資料
                        structured_detail = await detail_page.evaluate("""
                            () => {
                                const tables = Array.from(document.querySelectorAll('table'));
                                const text_content = document.body.innerText;
                                
                                return {
                                    tables_count: tables.length,
                                    page_text: text_content.slice(0, 2000),
                                    title: document.title,
                                    has_content: text_content.length > 100,
                                    url: window.location.href,
                                    content_length: text_content.length
                                };
                            }
                        """)
                        
                        await detail_page.close()
                        detail_page = None
                        
                        fetch_time = (datetime.now() - detail_start).total_seconds()
                        retry_suffix = f" (重試{attempt}次)" if attempt > 0 else ""
                        logger.info(f"    [{index+1}] 完成，{fetch_time:.1f}秒，{len(detail_content)//1024}KB{retry_suffix}")
                        
                        return {
                            **current_record,
                            "detail": {
                                "html": detail_content,
                                "structured": structured_detail,
                                "fetched": True,
                                "fetch_time_seconds": fetch_time,
                                "retry_count": attempt
                            }
                        }
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # 確保頁面被關閉
                    if detail_page and not detail_page.is_closed():
                        try:
                            await detail_page.close()
                        except:
                            pass
                        detail_page = None
                    
                    # 移除 dialog 監聽器
                    if dialog_handler:
                        try:
                            context.remove_listener("dialog", dialog_handler)
                        except:
                            pass
                    
                    # 如果是因為 alert 導致的錯誤，但我們已經處理了 alert，就不重試了
                    if alert_handled:
                        break
                    
                    # 判斷是否應該重試
                    should_retry = (
                        attempt < max_retries - 1 and
                        ("closed" in error_msg.lower() or 
                         "timeout" in error_msg.lower() or
                         "target page" in error_msg.lower() or
                         "context" in error_msg.lower())
                    )
                    
                    if should_retry:
                        retry_delay = min(2 ** attempt, 5)
                        logger.warning(f"    [{index+1}] 重試 {attempt+1}/{max_retries}: {error_msg}")
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        # 最終失敗
                        fetch_time = (datetime.now() - detail_start).total_seconds()
                        logger.error(f"    [{index+1}] 最終失敗 (重試{attempt}次): {error_msg}")
                        
                        # 確保 current_record 存在
                        if current_record is None:
                            current_record = {
                                'index': global_index,
                                'date': '',
                                'time': '',
                                'code': '',
                                'company': f'Unknown_{global_index}',
                                'subject': '',
                                'hasDetail': True
                            }
                        
                        return {
                            **current_record,
                            "detail": {
                                "error": error_msg,
                                "fetched": False,
                                "fetch_time_seconds": fetch_time,
                                "retry_count": attempt,
                                "final_failure": True,
                                "alert_message": alert_message if alert_message else None
                            }
                        }
    
    # 併發處理所有按鈕
    tasks = [
        _process_single_button(button, i) 
        for i, button in enumerate(buttons)
    ]
    
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 處理例外情況
    processed_results = []
    for i, result in enumerate(batch_results):
        if isinstance(result, Exception):
            logger.error(f"    批次項目 {i} 發生例外: {result}")
            global_index = offset + i
            
            # 安全獲取記錄
            current_record = None
            if global_index < len(main_data) and main_data[global_index] is not None:
                current_record = main_data[global_index]
            
            if current_record is None or not isinstance(current_record, dict):
                current_record = {
                    'index': global_index,
                    'date': '',
                    'time': '',
                    'code': '',
                    'company': f'Exception_{global_index}',
                    'subject': '',
                    'hasDetail': True
                }
            
            processed_results.append({
                **current_record,
                "detail": {
                    "error": str(result),
                    "fetched": False,
                    "fetch_time_seconds": 0,
                    "exception_in_batch": True
                }
            })
        else:
            processed_results.append(result)
    
    return processed_results


# --------------------
# MOPS 專用端點
# --------------------
@app.post("/mops-complete")
async def fetch_mops_complete(req: RenderRequest):
    """完整的 MOPS 抓取方案：主表 + 所有詳細資料，含詳細日誌"""
    
    start_time = datetime.now()
    logger.info(f"開始 MOPS 完整抓取: {req.url}")
    
    async def _complete_fetch():
        async with async_playwright() as p:
            logger.info("啟動瀏覽器...")
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = await browser.new_context(
                user_agent=req.user_agent,
                viewport={"width": 1920, "height": 1080},
            )
            page = await context.new_page()
            
            all_results = []
            
            try:
                # 載入主頁面
                logger.info("載入主頁面...")
                page_start = datetime.now()
                await page.goto(str(req.url), wait_until="networkidle", timeout=60000)
                await page.wait_for_selector("table", timeout=30000)
                page_load_time = (datetime.now() - page_start).total_seconds()
                logger.info(f"主頁面載入完成，耗時: {page_load_time:.2f}秒")
                
                # 取得主表格資料
                logger.info("解析主表格資料...")
                main_data = await get_main_data_safely(page)
                logger.info(f"主表格解析完成，找到 {len(main_data)} 筆記錄")
                
                # 為每筆資料取得詳細內容
                view_buttons = await page.query_selector_all('button:has-text("查看")')
                logger.info(f"找到 {len(view_buttons)} 個查看按鈕，開始抓取詳細資料...")
                
                # 估算總時間
                estimated_time = len(view_buttons) * 8  # 每個約8秒
                logger.info(f"預估總處理時間: {estimated_time//60}分{estimated_time%60}秒")
                
                for i, button in enumerate(view_buttons):
                    detail_start = datetime.now()
                    try:
                        # 顯示進度
                        progress = (i + 1) / len(view_buttons) * 100
                        logger.info(f"處理第 {i+1}/{len(view_buttons)} 個詳細頁面 ({progress:.1f}%)")
                        
                        if i < len(main_data):
                            company_info = f"{main_data[i].get('code', 'N/A')} {main_data[i].get('company', 'N/A')}"
                            logger.info(f"  公司: {company_info}")
                        
                        # 在新分頁中開啟詳細資料
                        logger.info(f"  點擊查看按鈕...")
                        async with context.expect_page() as new_page_info:
                            await button.click()
                            detail_page = await new_page_info.value
                        
                        logger.info(f"  等待詳細頁面載入...")
                        await detail_page.wait_for_load_state("networkidle", timeout=30000)
                        
                        # 取得詳細內容
                        detail_content = await detail_page.content()
                        content_size = len(detail_content)
                        logger.info(f"  取得內容大小: {content_size//1024}KB")
                        
                        # 嘗試解析結構化資料
                        structured_detail = await detail_page.evaluate("""
                            () => {
                                const tables = Array.from(document.querySelectorAll('table'));
                                const text_content = document.body.innerText;
                                
                                return {
                                    tables_count: tables.length,
                                    page_text: text_content.slice(0, 3000),
                                    title: document.title,
                                    has_content: text_content.length > 100,
                                    url: window.location.href,
                                    content_length: text_content.length
                                };
                            }
                        """)
                        
                        logger.info(f"  解析完成 - 表格數: {structured_detail.get('tables_count', 0)}, 內容長度: {structured_detail.get('content_length', 0)}")
                        
                        await detail_page.close()
                        
                        # 合併主資料和詳細資料
                        if i < len(main_data):
                            all_results.append({
                                **main_data[i],
                                "detail": {
                                    "html": detail_content,
                                    "structured": structured_detail,
                                    "fetched": True,
                                    "fetch_time_seconds": (datetime.now() - detail_start).total_seconds()
                                }
                            })
                        
                        detail_time = (datetime.now() - detail_start).total_seconds()
                        remaining = len(view_buttons) - (i + 1)
                        eta_seconds = remaining * detail_time
                        logger.info(f"  完成，耗時: {detail_time:.2f}秒，預估剩餘時間: {eta_seconds//60:.0f}分{eta_seconds%60:.0f}秒")
                        
                        # 避免請求過快
                        await page.wait_for_timeout(1000)
                        
                    except Exception as e:
                        error_msg = str(e)
                        logger.error(f"  錯誤: {error_msg}")
                        
                        if i < len(main_data):
                            all_results.append({
                                **main_data[i],
                                "detail": {
                                    "error": error_msg,
                                    "fetched": False,
                                    "fetch_time_seconds": (datetime.now() - detail_start).total_seconds()
                                }
                            })
                
                total_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"所有詳細頁面處理完成！總耗時: {total_time//60:.0f}分{total_time%60:.1f}秒")
                
            finally:
                logger.info("關閉瀏覽器...")
                await context.close()
                await browser.close()
            
            return all_results

    try:
        results = await _complete_fetch()
        
        # 統計結果
        success_count = sum(1 for r in results if r.get('detail', {}).get('fetched', False))
        total_time = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"抓取完成統計:")
        logger.info(f"  成功: {success_count}/{len(results)} 筆")
        logger.info(f"  總耗時: {total_time//60:.0f}分{total_time%60:.1f}秒")
        logger.info(f"  平均每筆: {total_time/len(results):.1f}秒" if results else "  無資料")
        
        return {
            "success": True,
            "total_records": len(results),
            "successful_details": success_count,
            "failed_details": len(results) - success_count,
            "total_time_seconds": total_time,
            "average_time_per_record": total_time / len(results) if results else 0,
            "data": results,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        total_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"抓取失敗: {str(e)}，耗時: {total_time:.1f}秒")
        raise HTTPException(status_code=502, detail=f"Complete fetch failed: {e}")


@app.post("/mops-concurrent")
async def fetch_mops_concurrent(req: RenderRequest):
    """併發版 MOPS 抓取：10個一組並行處理"""
    
    start_time = datetime.now()
    logger.info(f"開始 MOPS 併發抓取: {req.url}")
    
    # 併發設定
    BATCH_SIZE = 10
    MAX_CONCURRENT = 5
    
    async def _concurrent_fetch():
        async with async_playwright() as p:
            logger.info("啟動瀏覽器...")
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-web-security"]
            )
            context = await browser.new_context(
                user_agent=req.user_agent,
                viewport={"width": 1920, "height": 1080},
            )
            page = await context.new_page()
            
            try:
                # 載入主頁面
                logger.info("載入主頁面...")
                page_start = datetime.now()
                await page.goto(str(req.url), wait_until="networkidle", timeout=60000)
                await page.wait_for_selector("table", timeout=30000)
                page_load_time = (datetime.now() - page_start).total_seconds()
                logger.info(f"主頁面載入完成，耗時: {page_load_time:.2f}秒")
                
                # 取得主表格資料
                logger.info("解析主表格資料...")
                main_data = await get_main_data_safely(page)
                logger.info(f"主表格解析完成，找到 {len(main_data)} 筆有效記錄")
                
                # 只從有資料的列中找查看按鈕
                view_buttons = await page.query_selector_all('table tbody tr button:has-text("查看")')
                logger.info(f"找到 {len(view_buttons)} 個查看按鈕")
                
                # 估算總時間（併發版本）
                estimated_batches = (len(view_buttons) + BATCH_SIZE - 1) // BATCH_SIZE
                estimated_time = estimated_batches * 3
                logger.info(f"將分 {estimated_batches} 批處理，預估總時間: {estimated_time//60}分{estimated_time%60}秒")
                
                all_results = []
                
                # 分批併發處理
                for batch_start in range(0, len(view_buttons), BATCH_SIZE):
                    batch_end = min(batch_start + BATCH_SIZE, len(view_buttons))
                    batch_buttons = view_buttons[batch_start:batch_end]
                    batch_num = batch_start // BATCH_SIZE + 1
                    total_batches = (len(view_buttons) + BATCH_SIZE - 1) // BATCH_SIZE
                    
                    logger.info(f"處理第 {batch_num}/{total_batches} 批 ({len(batch_buttons)} 個項目)")
                    batch_start_time = datetime.now()
                    
                    # 併發處理這一批
                    batch_results = await _process_batch_concurrent(
                        context, batch_buttons, main_data, batch_start, MAX_CONCURRENT
                    )
                    
                    all_results.extend(batch_results)
                    
                    batch_time = (datetime.now() - batch_start_time).total_seconds()
                    remaining_batches = total_batches - batch_num
                    eta = remaining_batches * batch_time
                    
                    success_in_batch = sum(1 for r in batch_results if r.get('detail', {}).get('fetched', False))
                    logger.info(f"批次完成 - 成功: {success_in_batch}/{len(batch_results)}, 耗時: {batch_time:.1f}秒, 預估剩餘: {eta//60:.0f}分{eta%60:.0f}秒")
                    
                    # 批次間短暫休息
                    if batch_num < total_batches:
                        await asyncio.sleep(1)
                
                total_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"所有詳細頁面處理完成！總耗時: {total_time//60:.0f}分{total_time%60:.1f}秒")
                
                return all_results
                
            finally:
                logger.info("關閉瀏覽器...")
                await context.close()
                await browser.close()
    
    try:
        results = await _concurrent_fetch()
        
        # 統計結果
        success_count = sum(1 for r in results if r.get('detail', {}).get('fetched', False))
        total_time = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"併發抓取完成統計:")
        logger.info(f"  成功: {success_count}/{len(results)} 筆")
        logger.info(f"  總耗時: {total_time//60:.0f}分{total_time%60:.1f}秒")
        logger.info(f"  平均每筆: {total_time/len(results):.2f}秒" if results else "  無資料")
        
        return {
            "success": True,
            "total_records": len(results),
            "successful_details": success_count,
            "failed_details": len(results) - success_count,
            "total_time_seconds": total_time,
            "average_time_per_record": total_time / len(results) if results else 0,
            "data": results,
            "timestamp": datetime.now().isoformat(),
            "method": "concurrent",
            "batch_size": BATCH_SIZE
        }
    except Exception as e:
        total_time = (datetime.now() - start_time).total_seconds()
        logger.error(f"併發抓取失敗: {str(e)}，耗時: {total_time:.1f}秒")
        raise HTTPException(status_code=502, detail=f"Concurrent fetch failed: {e}")


@app.post("/mops-flexible")
async def fetch_mops_flexible(req: FlexibleMopsRequest):
    """靈活配置的 MOPS 抓取"""
    
    logger.info(f"靈活抓取設定 - 批次大小: {req.batch_size}, 最大併發: {req.max_concurrent}")
    
    start_time = datetime.now()
    
    async def _flexible_fetch():
        async with async_playwright() as p:
            logger.info("啟動瀏覽器...")
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-web-security"]
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
            )
            page = await context.new_page()
            
            try:
                # 載入主頁面
                await page.goto(str(req.url), wait_until="networkidle", timeout=60000)
                await page.wait_for_selector("table", timeout=30000)
                
                # 取得主表格資料
                main_data = await get_main_data_safely(page)
                logger.info(f"找到 {len(main_data)} 筆記錄")
                
                view_buttons = await page.query_selector_all('table tbody tr button:has-text("查看")')
                logger.info(f"找到 {len(view_buttons)} 個查看按鈕")
                
                all_results = []
                
                # 分批併發處理
                for batch_start in range(0, len(view_buttons), req.batch_size):
                    batch_end = min(batch_start + req.batch_size, len(view_buttons))
                    batch_buttons = view_buttons[batch_start:batch_end]
                    batch_num = batch_start // req.batch_size + 1
                    total_batches = (len(view_buttons) + req.batch_size - 1) // req.batch_size
                    
                    logger.info(f"處理第 {batch_num}/{total_batches} 批")
                    
                    batch_results = await _process_batch_concurrent(
                        context, batch_buttons, main_data, batch_start, req.max_concurrent
                    )
                    
                    all_results.extend(batch_results)
                    
                    if batch_num < total_batches:
                        await asyncio.sleep(1)
                
                return all_results
                
            finally:
                await context.close()
                await browser.close()
    
    try:
        results = await _flexible_fetch()
        success_count = sum(1 for r in results if r.get('detail', {}).get('fetched', False))
        total_time = (datetime.now() - start_time).total_seconds()
        
        return {
            "success": True,
            "total_records": len(results),
            "successful_details": success_count,
            "failed_details": len(results) - success_count,
            "total_time_seconds": total_time,
            "data": results,
            "timestamp": datetime.now().isoformat(),
            "method": "flexible",
            "batch_size": req.batch_size,
            "max_concurrent": req.max_concurrent
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Flexible fetch failed: {e}")


@app.post("/mops-quick-test")
async def fetch_mops_quick_test(req: RenderRequest):
    """快速測試版本：只抓取前3筆詳細資料"""
    
    logger.info(f"開始 MOPS 快速測試: {req.url}")
    
    async def _quick_test():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                logger.info("載入頁面...")
                await page.goto(str(req.url), wait_until="networkidle", timeout=60000)
                await page.wait_for_selector("table", timeout=30000)
                logger.info("頁面載入完成")
                
                view_buttons = await page.query_selector_all('button:has-text("查看")')
                test_count = min(3, len(view_buttons))
                logger.info(f"找到 {len(view_buttons)} 個按鈕，測試前 {test_count} 個")
                
                results = []
                for i in range(test_count):
                    logger.info(f"測試第 {i+1} 個按鈕...")
                    try:
                        async with context.expect_page() as new_page_info:
                            await view_buttons[i].click()
                            detail_page = await new_page_info.value
                        
                        await detail_page.wait_for_load_state("networkidle", timeout=20000)
                        content = await detail_page.content()
                        await detail_page.close()
                        
                        results.append({
                            "index": i,
                            "success": True,
                            "content_size": len(content),
                            "has_content": len(content) > 1000
                        })
                        logger.info(f"  成功，內容大小: {len(content)//1024}KB")
                        
                    except Exception as e:
                        logger.error(f"  失敗: {str(e)}")
                        results.append({"index": i, "success": False, "error": str(e)})
                
                return results
                
            finally:
                await context.close()
                await browser.close()
    
    results = await _quick_test()
    success_rate = sum(1 for r in results if r.get("success", False)) / len(results) * 100 if results else 0
    logger.info(f"快速測試完成，成功率: {success_rate:.1f}%")
    
    return {"test_results": results, "success_rate": success_rate}


@app.post("/mops-analyze-buttons")
async def analyze_mops_buttons(req: RenderRequest):
    """分析查看按鈕的實際連結或事件處理"""
    
    async def _analyze_buttons():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                await page.goto(str(req.url), wait_until="networkidle", timeout=60000)
                await page.wait_for_selector("table", timeout=30000)
                
                # 分析按鈕的屬性和事件
                buttons_info = await page.evaluate("""
                    () => {
                        const buttons = Array.from(document.querySelectorAll('button')).filter(btn => 
                            btn.textContent.includes('查看') || btn.hasAttribute('target')
                        );
                        
                        return buttons.map((btn, index) => {
                            const row = btn.closest('tr');
                            const cells = row ? Array.from(row.cells).map(cell => {
                                const span = cell.querySelector('span');
                                return span ? span.textContent.trim() : cell.textContent.trim();
                            }) : [];
                            
                            return {
                                index: index,
                                innerHTML: btn.innerHTML,
                                attributes: Object.fromEntries(
                                    Array.from(btn.attributes).map(attr => [attr.name, attr.value])
                                ),
                                onclick: btn.onclick ? btn.onclick.toString() : null,
                                rowData: cells,
                                parentHTML: btn.parentElement.innerHTML.slice(0, 200)
                            };
                        });
                    }
                """)
                
                return buttons_info
                
            finally:
                await context.close()
                await browser.close()

    result = await _analyze_buttons()
    return {"buttons_analysis": result}


# --------------------
# 健康檢查
# --------------------
@app.get("/healthz")
def healthz():
    return {"ok": True, "version": "1.4.1"}


# --------------------
# 主程式入口
# --------------------
if __name__ == "__main__":
    import uvicorn
    import os
    
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
