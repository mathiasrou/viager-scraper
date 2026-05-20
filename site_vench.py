# =========================================================
# VENCH SCRAPER
# VERSION ULTRA DEBUG FORENSIC
# =========================================================
#
# OBJECTIF :
#
# - comprendre EXACTEMENT ce que renvoie Vench
# - détecter blocages cloudflare
# - détecter lazy loading
# - détecter contenu JS
# - détecter redirections
# - détecter pages vides
# - détecter erreurs playwright
# - détecter anti-bot
# - récupérer TOUS les liens annonces
# - dumper HTML/TXT/screenshots
# - logger chaque étape
#
# =========================================================

import asyncio
import pandas as pd
import re
import os
import traceback
import json
import time

from datetime import datetime

from playwright.async_api import async_playwright

# =========================================================
# CONFIG
# =========================================================

BASE_URL = "https://www.vench.fr/prochaines-ventes-aux-encheres.html"

DEBUG_DIR = "debug_vench"

HEADLESS = False

MAX_PAGES = 3

os.makedirs(DEBUG_DIR, exist_ok=True)

# =========================================================
# UTILS
# =========================================================

def clean(txt):

    if txt is None:
        return ""

    txt = str(txt)

    txt = txt.replace("\n", " ")
    txt = txt.replace("\t", " ")
    txt = txt.replace("\xa0", " ")
    txt = txt.replace("\u202f", " ")

    txt = re.sub(r"\s+", " ", txt)

    return txt.strip()


def log(title, value=""):

    print("")
    print("=" * 80)
    print(title)

    if value != "":
        print(value)

    print("=" * 80)


def save_txt(path, content):

    with open(
        path,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(content)


# =========================================================
# EXTRACTIONS
# =========================================================

def extract_cp(txt):

    try:

        m = re.findall(r"\b(\d{5})\b", txt)

        if len(m) > 0:
            return m[0]

    except:
        pass

    return None


def extract_price(txt):

    try:

        matches = re.findall(
            r"(\d[\d\s]{2,})\s?€",
            txt
        )

        vals = []

        for m in matches:

            m = re.sub(r"\s+", "", m)

            if m.isdigit():

                v = int(m)

                if 1000 <= v <= 100000000:
                    vals.append(v)

        if len(vals) == 0:
            return None

        return min(vals)

    except:
        pass

    return None


def detect_type(txt):

    t = txt.lower()

    if "appartement" in t:
        return "Appartement"

    if "maison" in t:
        return "Maison"

    if "villa" in t:
        return "Villa"

    if "terrain" in t:
        return "Terrain"

    if "immeuble" in t:
        return "Immeuble"

    return "Autre"


# =========================================================
# MAIN SCRAPE
# =========================================================

async def scrape():

    rows = []

    seen = set()

    async with async_playwright() as p:

        # =====================================================
        # BROWSER
        # =====================================================

        log("🚀 LAUNCH BROWSER")

        browser = await p.chromium.launch(

            headless=HEADLESS,

            slow_mo=200,

            args=[

                "--disable-blink-features=AutomationControlled",

                "--disable-dev-shm-usage",

                "--no-sandbox",

                "--disable-setuid-sandbox",

                "--disable-infobars",

                "--window-size=1920,1080"

            ]
        )

        context = await browser.new_context(

            viewport={
                "width": 1920,
                "height": 1080
            },

            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),

            locale="fr-FR",

            timezone_id="Europe/Paris"
        )

        page = await context.new_page()

        detail = await context.new_page()

        # =====================================================
        # NETWORK DEBUG
        # =====================================================

        requests_log = []

        responses_log = []

        failures_log = []

        async def on_request(request):

            try:

                req = {
                    "method": request.method,
                    "url": request.url,
                    "resource": request.resource_type
                }

                requests_log.append(req)

                print(
                    f"➡️ REQUEST "
                    f"{request.resource_type} "
                    f"{request.method} "
                    f"{request.url[:120]}"
                )

            except:
                pass

        async def on_response(response):

            try:

                res = {
                    "status": response.status,
                    "url": response.url
                }

                responses_log.append(res)

                print(
                    f"⬅️ RESPONSE "
                    f"{response.status} "
                    f"{response.url[:120]}"
                )

            except:
                pass

        async def on_failed(request):

            try:

                fail = {
                    "url": request.url,
                    "failure": request.failure
                }

                failures_log.append(fail)

                print(
                    f"❌ FAILED "
                    f"{request.url[:120]}"
                )

            except:
                pass

        page.on("request", on_request)
        page.on("response", on_response)
        page.on("requestfailed", on_failed)

        # =====================================================
        # PAGES
        # =====================================================

        for page_num in range(1, MAX_PAGES + 1):

            log(f"📄 PAGE {page_num}")

            if page_num == 1:

                url = BASE_URL

            else:

                url = f"{BASE_URL}?p={page_num}"

            log("🌐 OPEN URL", url)

            t0 = time.time()

            try:

                response = await page.goto(

                    url,

                    wait_until="domcontentloaded",

                    timeout=180000
                )

                delta = round(time.time() - t0, 2)

                log("⏱️ LOAD TIME", f"{delta}s")

                if response:

                    log(
                        "📡 RESPONSE STATUS",
                        response.status
                    )

                else:

                    log("⚠️ NO RESPONSE OBJECT")

            except Exception as e:

                log("❌ GOTO ERROR", str(e))

                traceback.print_exc()

                continue

            # =================================================
            # ATTENTE LONGUE
            # =================================================

            log("⏳ WAIT EXTRA")

            await page.wait_for_timeout(15000)

            # =================================================
            # PAGE INFO
            # =================================================

            try:

                current_url = page.url

                title = await page.title()

                log("📍 FINAL URL", current_url)

                log("📄 TITLE", title)

            except Exception as e:

                log("❌ TITLE ERROR", str(e))

            # =================================================
            # HTML
            # =================================================

            try:

                html = await page.content()

                html_size = len(html)

                log("📦 HTML SIZE", html_size)

                save_txt(
                    f"{DEBUG_DIR}/page_{page_num}.html",
                    html
                )

            except Exception as e:

                log("❌ HTML ERROR", str(e))

            # =================================================
            # TXT
            # =================================================

            try:

                txt = await page.locator("body").inner_text()

                txt = clean(txt)

                txt_size = len(txt)

                log("📄 TXT SIZE", txt_size)

                print("")
                print(txt[:3000])

                save_txt(
                    f"{DEBUG_DIR}/page_{page_num}.txt",
                    txt
                )

            except Exception as e:

                log("❌ TXT ERROR", str(e))

            # =================================================
            # SCREENSHOT
            # =================================================

            try:

                await page.screenshot(

                    path=(
                        f"{DEBUG_DIR}/"
                        f"page_{page_num}.png"
                    ),

                    full_page=True
                )

                log("📸 SCREENSHOT OK")

            except Exception as e:

                log("❌ SCREENSHOT ERROR", str(e))

            # =================================================
            # DETECT CLOUDLFARE
            # =================================================

            try:

                checks = [

                    "cloudflare",
                    "checking your browser",
                    "captcha",
                    "verify you are human",
                    "ddos protection"

                ]

                low = txt.lower()

                for c in checks:

                    if c in low:

                        log(
                            "🚨 ANTI BOT DETECTE",
                            c
                        )

            except:
                pass

            # =================================================
            # LINKS
            # =================================================

            try:

                links = await page.locator("a").evaluate_all(
                    """
                    els => els.map(
                        e => ({
                            href:e.href,
                            txt:e.innerText
                        })
                    )
                    """
                )

                log(
                    "🔗 NB LINKS",
                    len(links)
                )

                save_txt(

                    f"{DEBUG_DIR}/links_{page_num}.json",

                    json.dumps(
                        links,
                        indent=2,
                        ensure_ascii=False
                    )
                )

            except Exception as e:

                log("❌ LINKS ERROR", str(e))

                links = []

            # =================================================
            # FILTRE ANNONCES
            # =================================================

            annonce_urls = []

            for l in links:

                href = l.get("href")

                if href is None:
                    continue

                href = str(href)

                if "/vente-" in href:

                    if href not in annonce_urls:

                        annonce_urls.append(href)

            log(
                "🏠 NB ANNONCES",
                len(annonce_urls)
            )

            for a in annonce_urls:

                print(a)

            # =================================================
            # DETAILS
            # =================================================

            for idx, annonce_url in enumerate(annonce_urls):

                try:

                    if annonce_url in seen:
                        continue

                    seen.add(annonce_url)

                    log(
                        f"🏠 DETAIL {idx+1}/{len(annonce_urls)}",
                        annonce_url
                    )

                    t1 = time.time()

                    response = await detail.goto(

                        annonce_url,

                        wait_until="domcontentloaded",

                        timeout=180000
                    )

                    dt = round(time.time() - t1, 2)

                    log(
                        "⏱️ DETAIL LOAD",
                        f"{dt}s"
                    )

                    await detail.wait_for_timeout(10000)

                    # =========================================
                    # TITLE
                    # =========================================

                    try:

                        title = await detail.title()

                        log(
                            "📄 DETAIL TITLE",
                            title
                        )

                    except Exception as e:

                        log(
                            "❌ DETAIL TITLE ERROR",
                            str(e)
                        )

                    # =========================================
                    # HTML
                    # =========================================

                    try:

                        detail_html = await detail.content()

                        save_txt(

                            f"{DEBUG_DIR}/"
                            f"detail_{idx}.html",

                            detail_html
                        )

                        log(
                            "📦 DETAIL HTML SIZE",
                            len(detail_html)
                        )

                    except Exception as e:

                        log(
                            "❌ DETAIL HTML ERROR",
                            str(e)
                        )

                    # =========================================
                    # TXT
                    # =========================================

                    try:

                        detail_txt = await detail.locator(
                            "body"
                        ).inner_text()

                        detail_txt = clean(detail_txt)

                        save_txt(

                            f"{DEBUG_DIR}/"
                            f"detail_{idx}.txt",

                            detail_txt
                        )

                        log(
                            "📄 DETAIL TXT SIZE",
                            len(detail_txt)
                        )

                        print("")
                        print(detail_txt[:5000])

                    except Exception as e:

                        log(
                            "❌ DETAIL TXT ERROR",
                            str(e)
                        )

                        detail_txt = ""

                    # =========================================
                    # SCREENSHOT
                    # =========================================

                    try:

                        await detail.screenshot(

                            path=(
                                f"{DEBUG_DIR}/"
                                f"detail_{idx}.png"
                            ),

                            full_page=True
                        )

                        log("📸 DETAIL SCREENSHOT OK")

                    except Exception as e:

                        log(
                            "❌ DETAIL SCREENSHOT ERROR",
                            str(e)
                        )

                    # =========================================
                    # EXTRACTIONS
                    # =========================================

                    row = {

                        "url": annonce_url,

                        "cp": extract_cp(detail_txt),

                        "prix": extract_price(detail_txt),

                        "type": detect_type(detail_txt),

                        "titre": clean(
                            detail_txt[:300]
                        ),

                        "txt_len": len(detail_txt)
                    }

                    log(
                        "✅ EXTRACTION",
                        json.dumps(
                            row,
                            indent=2,
                            ensure_ascii=False
                        )
                    )

                    rows.append(row)

                except Exception as e:

                    log(
                        "❌ DETAIL ERROR",
                        str(e)
                    )

                    traceback.print_exc()

        # =====================================================
        # SAVE NETWORK
        # =====================================================

        save_txt(

            f"{DEBUG_DIR}/requests.json",

            json.dumps(
                requests_log,
                indent=2,
                ensure_ascii=False
            )
        )

        save_txt(

            f"{DEBUG_DIR}/responses.json",

            json.dumps(
                responses_log,
                indent=2,
                ensure_ascii=False
            )
        )

        save_txt(

            f"{DEBUG_DIR}/failures.json",

            json.dumps(
                failures_log,
                indent=2,
                ensure_ascii=False
            )
        )

        log(
            "📡 REQUESTS TOTAL",
            len(requests_log)
        )

        log(
            "📡 RESPONSES TOTAL",
            len(responses_log)
        )

        log(
            "❌ FAILURES TOTAL",
            len(failures_log)
        )

        await browser.close()

    return pd.DataFrame(rows)


# =========================================================
# MAIN
# =========================================================

async def main():

    log("🚀 START")

    df = await scrape()

    log(
        "📊 DF SHAPE",
        df.shape
    )

    print(df.head())

    df.to_csv(

        "vench_debug.csv",

        sep=";",

        index=False,

        encoding="utf-8-sig"
    )

    log("💾 CSV SAVED")


asyncio.run(main())
