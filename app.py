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

app = FastAPI(title="Universal HTML Fetcher", version="1.1.0")


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
    base_name = req.filename or default_filename(str(req.url))
    if ("html" in origin_ct.lower()) and "." not in base_name.split("_")[-1]:
        base_name = f"{base_name}.html"

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

    base_name = req.filename or default_filename(str(req.url))
    if not base_name.endswith(".html"):
        base_name += ".html"
    ascii_name = base_name.encode("ascii", "ignore").decode("ascii") or "page.html"
    cd = f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{quote(base_name)}'

    return StreamingResponse(
        BytesIO(html.encode("utf-8")),
        media_type="text/html; charset=utf-8",
        headers={"Content-Disposition": cd},
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
