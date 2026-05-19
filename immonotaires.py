# =========================================================
# IMMO NOTAIRES ENCHERES
# VERSION DEBUG ULTIME
# OBJECTIF :
# - comprendre EXACTEMENT ce qui casse
# - logger DOM
# - logger API
# - logger JSON
# - logger HTML
# - logger JS
# - logger console navigateur
# - logger requêtes réseau
# - logger réponses
# =========================================================

import asyncio
import json
import traceback
import re
import os

from playwright.async_api import async_playwright


# =========================================================
# CONFIG
# =========================================================

BASE_URL = "https://immonotairesencheres.com/bien?page=0"

HEADLESS = True

DEBUG_DIR = "debug_immonotaires"

os.makedirs(
    DEBUG_DIR,
    exist_ok=True
)


# =========================================================
# HELPERS
# =========================================================

def clean(txt):

    if txt is None:
        return ""

    txt = str(txt)

    txt = txt.replace("\n", " ")
    txt = txt.replace("\t", " ")
    txt = txt.replace("\xa0", " ")

    txt = re.sub(r"\s+", " ", txt)

    return txt.strip()


def save_text(name, txt):

    path = os.path.join(
        DEBUG_DIR,
        name
    )

    with open(
        path,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(txt)

    print("💾", path)


# =========================================================
# NETWORK DEBUG
# =========================================================

async def handle_request(request):

    try:

        print("")
        print("➡️ REQUEST")
        print(request.method)
        print(request.url)

        if (
            "api" in request.url.lower()
            or "graphql" in request.url.lower()
            or "bien" in request.url.lower()
        ):

            print("🔥 API DETECTEE")

            headers = request.headers

            print("HEADERS :")

            for k, v in headers.items():

                print(k, ":", v)

            try:

                post = request.post_data

                if post:

                    print("")
                    print("POST DATA :")
                    print(post[:2000])

            except:
                pass

    except Exception as e:

        print(e)


async def handle_response(response):

    try:

        url = response.url

        ct = response.headers.get(
            "content-type",
            ""
        )

        print("")
        print("⬅️ RESPONSE")
        print(response.status)
        print(url)

        # =========================================
        # JSON
        # =========================================

        if "json" in ct.lower():

            print("🔥 JSON DETECTE")

            try:

                data = await response.json()

                txt = json.dumps(
                    data,
                    indent=2,
                    ensure_ascii=False
                )

                print(txt[:3000])

                filename = (
                    "json_" +
                    re.sub(r"[^a-zA-Z0-9]", "_", url)[:120]
                    + ".json"
                )

                save_text(
                    filename,
                    txt
                )

            except Exception as e:

                print("❌ JSON")
                print(e)

        # =========================================
        # HTML
        # =========================================

        elif "html" in ct.lower():

            try:

                body = await response.text()

                if (
                    "annonce" in body.lower()
                    or "prix" in body.lower()
                    or "surface" in body.lower()
                    or "/bien/" in body.lower()
                ):

                    print("🔥 HTML INTERESSANT")

                    filename = (
                        "html_" +
                        re.sub(r"[^a-zA-Z0-9]", "_", url)[:120]
                        + ".html"
                    )

                    save_text(
                        filename,
                        body[:500000]
                    )

            except:
                pass

    except Exception as e:

        print(e)


# =========================================================
# MAIN DEBUG
# =========================================================

async def main():

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=HEADLESS
        )

        context = await browser.new_context()

        page = await context.new_page()

        # =================================================
        # CONSOLE JS
        # =================================================

        page.on(
            "console",
            lambda msg:
            print(
                f"🖥️ CONSOLE [{msg.type}] :",
                msg.text
            )
        )

        # =================================================
        # ERREURS JS
        # =================================================

        page.on(
            "pageerror",
            lambda e:
            print(
                "❌ JS ERROR :",
                e
            )
        )

        # =================================================
        # REQUESTS
        # =================================================

        page.on(
            "request",
            lambda req:
            asyncio.create_task(
                handle_request(req)
            )
        )

        # =================================================
        # RESPONSES
        # =================================================

        page.on(
            "response",
            lambda res:
            asyncio.create_task(
                handle_response(res)
            )
        )

        # =================================================
        # NAVIGATION
        # =================================================

        print("")
        print("=" * 80)
        print("🚀 GOTO")
        print("=" * 80)

        response = await page.goto(
            BASE_URL,
            wait_until="networkidle",
            timeout=120000
        )

        print("")
        print("🌐 STATUS =", response.status)

        # =================================================
        # ATTENTE LONGUE
        # =================================================

        print("")
        print("⏳ ATTENTE JS")

        await page.wait_for_timeout(15000)

        # =================================================
        # SCROLL
        # =================================================

        print("")
        print("🖱️ SCROLL")

        for i in range(15):

            await page.mouse.wheel(
                0,
                5000
            )

            await page.wait_for_timeout(1500)

            print(
                "SCROLL",
                i + 1
            )

        # =================================================
        # HTML FINAL
        # =================================================

        print("")
        print("=" * 80)
        print("💾 HTML FINAL")
        print("=" * 80)

        html = await page.content()

        save_text(
            "final_page.html",
            html
        )

        print("")
        print("TAILLE HTML =", len(html))

        # =================================================
        # SCREENSHOT
        # =================================================

        print("")
        print("📸 SCREENSHOT")

        await page.screenshot(
            path=os.path.join(
                DEBUG_DIR,
                "full.png"
            ),
            full_page=True
        )

        # =================================================
        # BODY TEXT
        # =================================================

        print("")
        print("=" * 80)
        print("📄 BODY TEXT")
        print("=" * 80)

        body = await page.locator(
            "body"
        ).inner_text()

        body = clean(body)

        print(body[:5000])

        save_text(
            "body_text.txt",
            body
        )

        # =================================================
        # TEST SELECTEURS
        # =================================================

        print("")
        print("=" * 80)
        print("🔍 TEST SELECTEURS")
        print("=" * 80)

        selectors = [

            "article",
            "article.node-property",

            ".card",
            ".property",
            ".property-card",
            ".listing",
            ".annonce",

            "a[href*='/bien/']",

            "[class*='property']",
            "[class*='annonce']",
            "[class*='listing']",
            "[class*='card']",

            "[href*='/bien/']"

        ]

        for sel in selectors:

            try:

                els = await page.query_selector_all(
                    sel
                )

                print("")
                print(sel)
                print("COUNT =", len(els))

                if len(els) > 0:

                    for i, el in enumerate(els[:5]):

                        try:

                            txt = clean(
                                await el.inner_text()
                            )

                            print("")
                            print(f"--- ELEMENT {i} ---")

                            print(txt[:500])

                            try:

                                href = await el.get_attribute(
                                    "href"
                                )

                                if href:
                                    print("HREF =", href)

                            except:
                                pass

                        except Exception as e:

                            print(e)

            except Exception as e:

                print(e)

        # =================================================
        # JS VARIABLES
        # =================================================

        print("")
        print("=" * 80)
        print("🧠 VARIABLES JS")
        print("=" * 80)

        try:

            js_data = await page.evaluate(
                """
() => {

    const out = {}

    for (const k in window) {

        try {

            const v = window[k]

            if (
                typeof v === 'object'
                || typeof v === 'array'
            ) {

                const s = JSON.stringify(v)

                if (
                    s &&
                    (
                        s.includes('prix')
                        || s.includes('surface')
                        || s.includes('/bien/')
                        || s.includes('enchere')
                    )
                ) {

                    out[k] = s.slice(0, 5000)
                }
            }

        } catch(e) {}

    }

    return out
}
"""
            )

            txt = json.dumps(
                js_data,
                indent=2,
                ensure_ascii=False
            )

            print(txt[:5000])

            save_text(
                "window_variables.json",
                txt
            )

        except Exception as e:

            print(e)

        # =================================================
        # LOCAL STORAGE
        # =================================================

        print("")
        print("=" * 80)
        print("💾 LOCAL STORAGE")
        print("=" * 80)

        try:

            storage = await page.evaluate(
                """
() => {

    const out = {}

    for (let i = 0; i < localStorage.length; i++) {

        const k = localStorage.key(i)

        out[k] = localStorage.getItem(k)
    }

    return out
}
"""
            )

            txt = json.dumps(
                storage,
                indent=2,
                ensure_ascii=False
            )

            print(txt)

            save_text(
                "local_storage.json",
                txt
            )

        except Exception as e:

            print(e)

        # =================================================
        # COOKIES
        # =================================================

        print("")
        print("=" * 80)
        print("🍪 COOKIES")
        print("=" * 80)

        try:

            cookies = await context.cookies()

            txt = json.dumps(
                cookies,
                indent=2,
                ensure_ascii=False
            )

            print(txt)

            save_text(
                "cookies.json",
                txt
            )

        except Exception as e:

            print(e)

        # =================================================
        # FIN
        # =================================================

        print("")
        print("=" * 80)
        print("✅ FIN DEBUG")
        print("=" * 80)

        await browser.close()


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    try:

        asyncio.run(main())

    except Exception as e:

        print("")
        print("=" * 80)
        print("❌ ERREUR FATALE")
        print("=" * 80)

        print(e)

        traceback.print_exc()
