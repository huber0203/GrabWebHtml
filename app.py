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

app = FastAPI(title="Universal HTML Fetcher", version="1.2.0")


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
# /fetch 端點：純 HTTP 抓取
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


@app.get("/healthz")
def healthz():
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    import os
    
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
