# =========================================================
# AVOVENTES SCRAPER
# ULTRA DEBUG VERSION
# =========================================================

import asyncio
import pandas as pd
import re
import traceback
from playwright.async_api import async_playwright


URL = "https://avoventes.fr/recherche/toutes"


# =========================================================
# CLEAN
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


# =========================================================
# PRICE
# =========================================================

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
        return None


# =========================================================
# CP
# =========================================================

def extract_cp(txt):

    try:

        m = re.findall(
            r"\b(\d{5})\b",
            txt
        )

        if len(m) > 0:
            return m[0]

    except:
        pass

    return None


# =========================================================
# TYPE
# =========================================================

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

    if "studio" in t:
        return "Studio"

    if "garage" in t:
        return "Garage"

    return "Autre"


# =========================================================
# DEBUG ELEMENT
# =========================================================

async def debug_selector(page, selector):

    print("")
    print("================================================")
    print(f"🔍 SELECTOR DEBUG : {selector}")
    print("================================================")

    try:

        count = await page.locator(selector).count()

        print(f"🧩 COUNT = {count}")

        if count > 0:

            for i in range(min(count, 5)):

                el = page.locator(selector).nth(i)

                txt = clean(
                    await el.inner_text()
                )

                html = clean(
                    await el.inner_html()
                )

                print("")
                print(f"---------- ELEMENT {i+1} ----------")

                print("📝 TEXT:")
                print(txt[:1000])

                print("")
                print("💾 HTML:")
                print(html[:1000])

    except Exception as e:

        print("❌ ERREUR SELECTOR")
        print(e)


# =========================================================
# MAIN SCRAPE
# =========================================================

async def scrape():

    rows = []

    seen = set()

    print("")
    print("================================================")
    print("🚀 START")
    print("================================================")

    async with async_playwright() as p:

        # =================================================
        # BROWSER
        # =================================================

        print("")
        print("================================================")
        print("🌐 OPEN BROWSER")
        print("================================================")

        browser = await p.chromium.launch(

            headless=False,

            args=[

                "--no-sandbox",
                "--disable-dev-shm-usage"

            ]
        )

        # =================================================
        # CONTEXT
        # =================================================

        print("")
        print("================================================")
        print("🧠 CONTEXT")
        print("================================================")

        context = await browser.new_context(

            viewport={
                "width": 1800,
                "height": 1200
            },

            locale="fr-FR",

            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )

        # =================================================
        # PAGE
        # =================================================

        page = await context.new_page()

        # =================================================
        # REQUESTS
        # =================================================

        async def log_request(request):

            try:

                url = request.url

                if any(x in url.lower() for x in [

                    "api",
                    "graphql",
                    "json",
                    "search",
                    "vente",
                    "annonce"

                ]):

                    print("")
                    print("================================================")
                    print("📡 REQUEST")
                    print("================================================")

                    print(url)

            except:
                pass

        page.on(
            "request",
            log_request
        )

        # =================================================
        # RESPONSE
        # =================================================

        async def log_response(response):

            try:

                url = response.url

                if any(x in url.lower() for x in [

                    "api",
                    "graphql",
                    "json",
                    "search",
                    "vente",
                    "annonce"

                ]):

                    print("")
                    print("================================================")
                    print("📥 RESPONSE")
                    print("================================================")

                    print(url)

                    ct = response.headers.get(
                        "content-type",
                        ""
                    )

                    print(f"CONTENT TYPE = {ct}")

                    if "json" in ct:

                        try:

                            data = await response.text()

                            print("")
                            print("📦 JSON:")
                            print(data[:3000])

                        except:
                            pass

            except:
                pass

        page.on(
            "response",
            log_response
        )

        # =================================================
        # OPEN
        # =================================================

        print("")
        print("================================================")
        print("🌐 OPEN URL")
        print("================================================")

        print(URL)

        await page.goto(

            URL,

            timeout=120000,

            wait_until="networkidle"
        )

        # =================================================
        # WAIT
        # =================================================

        print("")
        print("================================================")
        print("⏳ WAIT")
        print("================================================")

        await page.wait_for_timeout(8000)

        # =================================================
        # COOKIE
        # =================================================

        print("")
        print("================================================")
        print("🍪 COOKIE")
        print("================================================")

        buttons = [

            "button:has-text('Tout accepter')",
            "button:has-text('Accepter')",
            "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"

        ]

        for b in buttons:

            try:

                await page.locator(b).click(timeout=3000)

                print(f"✅ CLICK COOKIE = {b}")

                break

            except:
                pass

        await page.wait_for_timeout(4000)

        # =================================================
        # PAGE TITLE
        # =================================================

        print("")
        print("================================================")
        print("📄 PAGE INFO")
        print("================================================")

        print(await page.title())

        # =================================================
        # BODY
        # =================================================

        body = await page.locator("body").inner_text()

        body = clean(body)

        print("")
        print("================================================")
        print("📄 BODY PREVIEW")
        print("================================================")

        print(body[:5000])

        # =================================================
        # SCROLL
        # =================================================

        print("")
        print("================================================")
        print("🖱️ SCROLL")
        print("================================================")

        for i in range(20):

            print(f"🖱️ {i+1}/20")

            await page.evaluate("""

window.scrollBy(
    0,
    window.innerHeight
)

""")

            await page.wait_for_timeout(2500)

        # =================================================
        # END
        # =================================================

        print("")
        print("================================================")
        print("🔚 END")
        print("================================================")

        await page.keyboard.press("End")

        await page.wait_for_timeout(5000)

        # =================================================
        # SCREENSHOT
        # =================================================

        print("")
        print("================================================")
        print("🖼️ SCREENSHOT")
        print("================================================")

        await page.screenshot(

            path="debug_avoventes.png",

            full_page=True
        )

        print("✅ SCREENSHOT SAVED")

        # =================================================
        # HTML
        # =================================================

        print("")
        print("================================================")
        print("💾 SAVE HTML")
        print("================================================")

        html = await page.content()

        with open(
            "debug_avoventes.html",
            "w",
            encoding="utf-8"
        ) as f:

            f.write(html)

        print("✅ HTML SAVED")

        # =================================================
        # SELECTORS DEBUG
        # =================================================

        selectors = [

            "article",
            ".card",
            ".item",
            ".listing",
            ".property",
            ".vente",
            ".annonce",
            ".enchere",
            ".search-result",
            ".result",
            ".grid-item",
            ".tile",
            "a",
            "div",
            "section",
            "li"

        ]

        for s in selectors:

            await debug_selector(
                page,
                s
            )

        # =================================================
        # ALL LINKS
        # =================================================

        print("")
        print("================================================")
        print("🔗 ALL LINKS")
        print("================================================")

        links = await page.locator("a").evaluate_all("""

els => els.map(e => ({

    href: e.href || "",
    text: e.innerText || "",
    html: e.innerHTML || ""

}))

""")

        print(f"🔗 TOTAL = {len(links)}")

        # =================================================
        # LOOP LINKS
        # =================================================

        for idx, item in enumerate(links):

            try:

                print("")
                print("################################################")
                print(f"🔎 LINK {idx+1}/{len(links)}")
                print("################################################")

                url = clean(item["href"])
                txt = clean(item["text"])
                html = clean(item["html"])

                print("")
                print("🌐 URL")
                print(url)

                print("")
                print("📝 TEXT")
                print(txt[:2000])

                print("")
                print("💾 HTML")
                print(html[:2000])

                # =========================================
                # FILTER URL
                # =========================================

                if "avoventes.fr" not in url:

                    print("⛔ BAD DOMAIN")

                    continue

                # =========================================
                # BAD LINKS
                # =========================================

                bad = [

                    "#",
                    "facebook",
                    "linkedin",
                    "twitter",
                    "youtube",
                    "mentions",
                    "privacy",
                    "conditions",
                    "contact",
                    "admin",
                    "login",
                    "friendlycaptcha",
                    "recherche/toutes"

                ]

                if any(x in url.lower() for x in bad):

                    print("⛔ FILTER URL")

                    continue

                # =========================================
                # KEYWORDS
                # =========================================

                combined = (
                    txt + " " + html
                ).lower()

                kws = [

                    "appartement",
                    "maison",
                    "villa",
                    "terrain",
                    "immeuble",
                    "studio",
                    "garage",
                    "local"

                ]

                if not any(k in combined for k in kws):

                    print("⛔ NO KEYWORD")

                    continue

                # =========================================
                # DUPLICATE
                # =========================================

                if url in seen:

                    print("⛔ DUPLICATE")

                    continue

                seen.add(url)

                # =========================================
                # ROW
                # =========================================

                row = {

                    "url": url,

                    "type": detect_type(combined),

                    "prix": extract_price(combined),

                    "cp": extract_cp(combined),

                    "texte": combined[:5000]

                }

                rows.append(row)

                print("")
                print("✅ VALID ANNOUNCE")

                print(row)

            except Exception as e:

                print("")
                print("❌ LINK ERROR")
                print(e)

        # =================================================
        # CLOSE
        # =================================================

        await browser.close()

    # =====================================================
    # DF
    # =====================================================

    print("")
    print("================================================")
    print("📦 DATAFRAME")
    print("================================================")

    df = pd.DataFrame(rows)

    if len(df) > 0:

        df = df.drop_duplicates(
            subset=["url"]
        )

    print(df)

    print("")
    print(f"📦 TOTAL = {len(df)}")

    return df


# =========================================================
# MAIN
# =========================================================

async def main():

    try:

        df = await scrape()

        if len(df) > 0:

            df.to_csv(

                "avoventes.csv",

                sep=";",

                index=False,

                encoding="utf-8-sig"
            )

            print("")
            print("================================================")
            print("✅ CSV SAVED")
            print("================================================")

        else:

            print("")
            print("================================================")
            print("❌ NO ANNOUNCES")
            print("================================================")

    except Exception as e:

        print("")
        print("================================================")
        print("❌ MAIN ERROR")
        print("================================================")

        print(e)

        traceback.print_exc()


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
