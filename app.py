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

app = FastAPI(title="Universal HTML Fetcher", version="1.6.0")

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
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    unwanted_tags = ['script', 'style', 'nav', 'header', 'footer', 'aside', 'iframe', 'embed', 'object', 'applet']
    for tag_name in unwanted_tags:
        for tag in soup.find_all(tag_name):
            tag.decompose()
    
    ad_patterns = ['gpt-ad', 'google-ad', 'adsense', 'adsbygoogle']
    for pattern in ad_patterns:
        for tag in soup.find_all(attrs={'class': re.compile(pattern, re.I)}):
            tag.decompose()
        for tag in soup.find_all(attrs={'id': re.compile(pattern, re.I)}):
            tag.decompose()
    
    for element in soup.find_all(text=True):
        if isinstance(element, str) and len(element) > 200:
            code_indicators = ['const ', 'var ', 'function(', 'googletag', 'window.', 'document.']
            if any(indicator in element for indicator in code_indicators):
                element.replace_with('')
    
    for tag in soup.find_all(True):
        attrs_to_remove = [attr for attr in tag.attrs if attr.lower().startswith('on')]
        for attr in attrs_to_remove:
            del tag[attr]
        if 'style' in tag.attrs and len(tag['style']) > 100:
            del tag['style']
    
    for form_tag in soup.find_all('form'):
        form_tag.decompose()
    
    for input_tag in soup.find_all('input'):
        if input_tag.get('type') not in ['hidden']:
            input_tag.decompose()
    
    for table in soup.find_all('table'):
        for tag in table.find_all(True):
            keep_attrs = ['class', 'id', 'colspan', 'rowspan', 'bgcolor', 'align', 'valign']
            attrs_to_remove = [attr for attr in tag.attrs if attr not in keep_attrs]
            for attr in attrs_to_remove:
                del tag[attr]
    
    for tag in soup.find_all():
        if (not tag.get_text(strip=True) and not tag.find('img') and not tag.find('table') and
            not tag.find('select') and tag.name not in ['br', 'hr', 'tr', 'td', 'th']):
            tag.decompose()
    
    cleaned_html = re.sub(r'\n\s*\n\s*\n', '\n\n', str(soup))
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
    wait_selector: Optional[str] = Field(None, description="等待指定 selector 再抓取，例如 'table'")
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
            req.method, str(req.url), params=req.params, data=req.data,
            timeout=req.timeout, verify=not req.insecure, allow_redirects=True,
        )
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Upstream request failed: {e}")

    content_bytes = resp.content
    origin_ct = resp.headers.get("Content-Type", "application/octet-stream")
    
    if req.pretty_content and "html" in origin_ct.lower():
        try:
            html_content = content_bytes.decode(resp.encoding or 'utf-8')
            content_bytes = clean_html_content(html_content).encode('utf-8')
            origin_ct = "text/html; charset=utf-8"
        except Exception:
            pass
    
    base_name = req.filename or default_filename(str(req.url))
    if ("html" in origin_ct.lower()) and "." not in base_name.split("_")[-1]:
        base_name = f"{base_name}.html"
    
    if req.pretty_content and "html" in origin_ct.lower():
        base_name = base_name.replace('.html', '_cleaned.html')

    ascii_name = base_name.encode("ascii", "ignore").decode("ascii") or "download.html"
    cd = f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{quote(base_name)}'

    return StreamingResponse(
        BytesIO(content_bytes),
        media_type=origin_ct,
        headers={
            "Content-Disposition": cd, "X-Origin-Status": str(resp.status_code),
            "X-Origin-URL": str(resp.url),
            **({"X-Origin-Encoding": resp.encoding} if resp.encoding else {}),
            **({"X-Content-Cleaned": "true"} if req.pretty_content else {}),
        },
    )

# --------------------
# /render 端點：JS 渲染抓取
# --------------------
@app.post("/render")
async def render(req: RenderRequest):
    async def _once():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = await browser.new_context(
                user_agent=req.user_agent, java_script_enabled=not req.disable_js,
                viewport=req.viewport or {"width": 1366, "height": 900},
                extra_http_headers={"referer": req.referer or "", "accept-language": "zh-TW,zh;q=0.9,en;q=0.8"},
            )
            if req.cookies:
                await context.add_cookies([{"name": k, "value": v, "url": str(req.url)} for k, v in req.cookies.items()])

            page = await context.new_page()
            if req.referer:
                try:
                    await page.goto(req.referer, wait_until="domcontentloaded", timeout=min(15000, req.timeout_ms))
                except Exception:
                    pass

            await page.goto(str(req.url), wait_until=req.wait_until, timeout=req.timeout_ms)
            if req.wait_selector:
                await page.wait_for_selector(req.wait_selector, timeout=req.timeout_ms, state="visible")

            html = await page.content()
            await context.close()
            await browser.close()
            return html

    try:
        html = await _once()
    except PWTimeout:
        logger.info("Timeout, retrying with 'load' wait_until state...")
        req.wait_until = "load"
        req.timeout_ms = int(req.timeout_ms * 1.5)
        html = await _once()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Render failed: {e}")

    if req.pretty_content:
        try:
            html = clean_html_content(html)
        except Exception:
            pass

    base_name = req.filename or default_filename(str(req.url))
    if not base_name.endswith(".html"):
        base_name += ".html"
    
    if req.pretty_content:
        base_name = base_name.replace('.html', '_cleaned.html')
        
    ascii_name = base_name.encode("ascii", "ignore").decode("ascii") or "page.html"
    cd = f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{quote(base_name)}'

    return StreamingResponse(
        BytesIO(html.encode("utf-8")),
        media_type="text/html; charset=utf-8",
        headers={"Content-Disposition": cd, **({"X-Content-Cleaned": "true"} if req.pretty_content else {})},
    )

# --------------------
# MOPS 安全資料解析函數
# --------------------
async def get_main_data_safely(page):
    """安全地獲取主表格資料，確保返回的資料是可序列化的純基本類型"""
    try:
        main_data = await page.evaluate("""
            () => {
                const dataRows = document.querySelectorAll('table tbody tr');
                const results = [];
                dataRows.forEach(row => {
                    try {
                        const cells = Array.from(row.cells);
                        const viewButton = row.querySelector('button');
                        const hasViewButton = viewButton && viewButton.textContent.includes('查看');
                        if (cells.length >= 6 && hasViewButton) {
                            results.push({
                                index: results.length,
                                date: String((cells[0]?.textContent || '').trim()),
                                time: String((cells[1]?.textContent || '').trim()),
                                code: String((cells[2]?.textContent || '').trim()),
                                company: String((cells[3]?.textContent || '').trim()) || `Company_${results.length}`,
                                subject: String((cells[4]?.textContent || '').trim()),
                                hasDetail: Boolean(hasViewButton)
                            });
                        }
                    } catch (e) { console.error('Error parsing a row:', e); }
                });
                return results;
            }
        """)
    except Exception as e:
        logger.error(f"主資料解析失敗: {e}")
        main_data = []
    
    logger.info(f"安全解析完成 - 找到 {len(main_data)} 筆有效記錄")
    if main_data:
        logger.info(f"前3筆資料預覽: {main_data[:3]}")
    return main_data

# --------------------
# MOPS 專用端點
# --------------------
async def _fetch_mops_details_concurrently(req: FlexibleMopsRequest, page, context):
    """使用 Locator API 併發抓取 MOPS 詳細資料的核心函式"""
    main_data = await get_main_data_safely(page)
    locator = page.locator('table tbody tr button:has-text("查看")')
    button_count = await locator.count()
    logger.info(f"找到 {button_count} 個查看按鈕，準備併發處理...")

    semaphore = asyncio.Semaphore(req.max_concurrent)

    async def _process_single_button(index):
        async with semaphore:
            record = main_data[index] if index < len(main_data) else {'company': f'Unknown_{index}', 'date': 'N/A'}
            company_name = record.get('company', 'N/A')
            data_date = record.get('date', 'N/A')
            logger.info(f"  處理 {index + 1}/{button_count}: {company_name} ({data_date})")
            
            detail_start = datetime.now()
            try:
                async with context.expect_page(timeout=15000) as page_info:
                    await locator.nth(index).click(timeout=10000)
                detail_page = await page_info.value
                
                try:
                    await detail_page.wait_for_load_state("networkidle", timeout=20000)
                    detail_content = await detail_page.content()
                    structured = await detail_page.evaluate("() => ({ title: document.title, url: window.location.href, text_length: document.body.innerText.length })")
                    await detail_page.close()
                    
                    fetch_time = (datetime.now() - detail_start).total_seconds()
                    logger.info(f"    -> 成功, {fetch_time:.1f}s, {len(detail_content)//1024}KB")
                    return {**record, "detail": {"html": detail_content, "structured": structured, "fetched": True, "fetch_time_seconds": fetch_time}}
                except Exception as page_e:
                    logger.warning(f"    -> 頁面處理失敗: {str(page_e)[:100]}")
                    await detail_page.close()
                    raise page_e

            except Exception as e:
                # 可能是 alert 或點擊超時
                fetch_time = (datetime.now() - detail_start).total_seconds()
                if "expect_page" in str(e).lower():
                    logger.info(f"    -> Alert 或無新分頁, {fetch_time:.1f}s")
                    return {**record, "detail": {"fetched": True, "page_type": "alert_or_none"}}
                else:
                    logger.error(f"    -> 點擊失敗: {str(e)[:100]}")
                    return {**record, "detail": {"fetched": False, "error": str(e)}}

    tasks = [_process_single_button(i) for i in range(button_count)]
    return await asyncio.gather(*tasks)


@app.post("/mops-flexible")
async def fetch_mops_flexible(req: FlexibleMopsRequest):
    """靈活配置的 MOPS 抓取 (使用 Locator API 提高穩定性)"""
    logger.info(f"靈活抓取設定 - 批次大小: {req.batch_size}, 最大併發: {req.max_concurrent}")
    start_time = datetime.now()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        page = await context.new_page()
        try:
            await page.goto(req.url, wait_until="networkidle", timeout=60000)
            await page.wait_for_selector("table", timeout=30000)
            results = await _fetch_mops_details_concurrently(req, page, context)
        except Exception as e:
            logger.error(f"Flexible fetch 發生致命錯誤: {e}", exc_info=True)
            raise HTTPException(status_code=502, detail=f"Flexible fetch failed: {e}")
        finally:
            await browser.close()

    total_time = (datetime.now() - start_time).total_seconds()
    success_count = sum(1 for r in results if r.get('detail', {}).get('fetched'))
    logger.info(f"抓取完成 - 成功: {success_count}/{len(results)}, 總耗時: {total_time:.1f}s")

    return {
        "success": True, "total_records": len(results), "successful_details": success_count,
        "failed_details": len(results) - success_count, "total_time_seconds": total_time,
        "data": results, "timestamp": datetime.now().isoformat(),
    }

@app.post("/mops-quick-test")
async def fetch_mops_quick_test(req: RenderRequest):
    """快速測試版本：使用新方法抓取前3筆詳細資料"""
    logger.info(f"開始 MOPS 快速測試: {req.url}")
    mops_req = FlexibleMopsRequest(url=str(req.url), max_concurrent=3)
    
    # 修改 mops_req 的 url 屬性以進行測試
    setattr(mops_req, 'url', str(req.url))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = await browser.new_context()
        page = await context.new_page()
        try:
            await page.goto(mops_req.url, wait_until="networkidle", timeout=60000)
            main_data = await get_main_data_safely(page)
            locator = page.locator('table tbody tr button:has-text("查看")')
            test_count = min(3, await locator.count())
            
            # 建立一個只包含前幾項的 locator
            test_locator = locator.first if test_count > 0 else None
            for i in range(1, test_count):
                test_locator = test_locator.or_(locator.nth(i))

            if test_locator:
                 # 重新實作一個簡化版的併發抓取邏輯
                semaphore = asyncio.Semaphore(mops_req.max_concurrent)
                async def _process_test(index):
                    async with semaphore:
                        try:
                            async with context.expect_page(timeout=15000) as page_info:
                                await locator.nth(index).click()
                            detail_page = await page_info.value
                            content = await detail_page.content()
                            await detail_page.close()
                            return {"index": index, "success": True, "content_size": len(content)}
                        except Exception as e:
                            return {"index": index, "success": False, "error": str(e)}
                tasks = [_process_test(i) for i in range(test_count)]
                results = await asyncio.gather(*tasks)
            else:
                results = []

        finally:
            await browser.close()
    
    success_rate = sum(1 for r in results if r["success"]) / len(results) * 100 if results else 0
    return {"test_results": results, "success_rate": success_rate}

# --------------------
# 健康檢查
# --------------------
@app.get("/healthz")
def healthz():
    return {"ok": True, "version": "1.6.0"}

# --------------------
# 主程式入口
# --------------------
if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)

