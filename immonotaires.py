# =========================================================
# IMMONOTAIRES DEBUG EXTREME
# TEST DE TOUTES LES SOLUTIONS ANTI-BLOCAGE
# =========================================================

import asyncio
import random
import traceback
import os

from playwright.async_api import async_playwright


# =========================================================
# URLS
# =========================================================

URLS = [

    "https://immonotairesencheres.com",
    "https://www.immonotairesencheres.com",
    "https://immonotairesencheres.com/recherche",
    "https://immonotairesencheres.com/bien"

]


# =========================================================
# USER AGENTS
# =========================================================

USER_AGENTS = [

    # Chrome Windows
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),

    # Firefox Windows
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) "
        "Gecko/20100101 Firefox/136.0"
    ),

    # Edge
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
    ),

    # Chrome Linux
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    )

]


# =========================================================
# JS STEALTH
# =========================================================

STEALTH_JS = """

Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined
});

window.chrome = {
    runtime: {}
};

Object.defineProperty(navigator, 'plugins', {
    get: () => [1, 2, 3, 4, 5]
});

Object.defineProperty(navigator, 'languages', {
    get: () => ['fr-FR', 'fr']
});

Object.defineProperty(navigator, 'platform', {
    get: () => 'Win32'
});

"""


# =========================================================
# TEST
# =========================================================

async def test_combo(
    playwright,
    headless,
    channel,
    user_agent,
    use_stealth,
    url
):

    browser = None

    try:

        print("")
        print("=" * 80)

        print("🧪 TEST")

        print(f"HEADLESS = {headless}")
        print(f"CHANNEL = {channel}")
        print(f"STEALTH = {use_stealth}")
        print(f"URL = {url}")

        print("=" * 80)

        launch_args = [

            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-setuid-sandbox"

        ]

        # =====================================================
        # BROWSER
        # =====================================================

        browser = await playwright.chromium.launch(

            headless=headless,

            channel=channel,

            args=launch_args

        )

        # =====================================================
        # CONTEXT
        # =====================================================

        context = await browser.new_context(

            user_agent=user_agent,

            locale="fr-FR",

            timezone_id="Europe/Paris",

            viewport={
                "width": 1600,
                "height": 1200
            },

            extra_http_headers={

                "Accept-Language":
                    "fr-FR,fr;q=0.9,en;q=0.8",

                "Upgrade-Insecure-Requests":
                    "1"

            }

        )

        # =====================================================
        # PAGE
        # =====================================================

        page = await context.new_page()

        # =====================================================
        # STEALTH
        # =====================================================

        if use_stealth:

            await page.add_init_script(
                STEALTH_JS
            )

        # =====================================================
        # LOGS
        # =====================================================

        async def log_response(response):

            try:

                ct = response.headers.get(
                    "content-type",
                    ""
                )

                print("")
                print("📡 RESPONSE")
                print("URL =", response.url)
                print("STATUS =", response.status)
                print("TYPE =", ct)

            except:
                pass

        page.on(
            "response",
            log_response
        )

        # =====================================================
        # WAIT RANDOM
        # =====================================================

        await page.wait_for_timeout(
            random.randint(1000, 3000)
        )

        # =====================================================
        # GOTO
        # =====================================================

        print("")
        print("🌐 OPEN")

        response = await page.goto(

            url,

            timeout=120000,

            wait_until="domcontentloaded"

        )

        # =====================================================
        # WAIT
        # =====================================================

        await page.wait_for_timeout(
            random.randint(5000, 10000)
        )

        # =====================================================
        # TITLE
        # =====================================================

        try:

            title = await page.title()

        except:

            title = "ERROR"

        print("")
        print("📄 TITLE =", title)

        # =====================================================
        # URL
        # =====================================================

        print("🔗 FINAL URL =", page.url)

        # =====================================================
        # HTML SIZE
        # =====================================================

        try:

            html = await page.content()

            print("📦 HTML SIZE =", len(html))

        except Exception as e:

            print("❌ HTML ERROR")
            print(e)

            html = ""

        # =====================================================
        # BODY
        # =====================================================

        try:

            body = await page.locator("body").inner_text()

            print("")
            print("📝 BODY SAMPLE")
            print(body[:5000])

        except Exception as e:

            print("❌ BODY ERROR")
            print(e)

            body = ""

        # =====================================================
        # SCREENSHOT
        # =====================================================

        screenshot_name = (

            f"debug_"
            f"{'headless' if headless else 'headful'}_"
            f"{channel}_"
            f"{'stealth' if use_stealth else 'normal'}.png"
        )

        await page.screenshot(

            path=screenshot_name,

            full_page=True

        )

        print("")
        print("📸 SCREENSHOT =", screenshot_name)

        # =====================================================
        # SAVE HTML
        # =====================================================

        html_name = (

            f"debug_"
            f"{'headless' if headless else 'headful'}_"
            f"{channel}_"
            f"{'stealth' if use_stealth else 'normal'}.html"
        )

        with open(
            html_name,
            "w",
            encoding="utf-8"
        ) as f:

            f.write(html)

        print("💾 HTML =", html_name)

        # =====================================================
        # DETECTION SUCCESS
        # =====================================================

        success = False

        indicators = [

            "encheres",
            "vente",
            "notaire",
            "recherche",
            "bien"

        ]

        low = body.lower()

        for ind in indicators:

            if ind in low:

                success = True
                break

        # =====================================================
        # 403
        # =====================================================

        forbidden = (

            "403" in low
            or "accès interdit" in low
            or "forbidden" in low
        )

        print("")
        print("🚨 403 =", forbidden)

        print("✅ SUCCESS =", success)

        # =====================================================
        # LINKS
        # =====================================================

        try:

            links = await page.query_selector_all("a")

            print("🔗 LINKS =", len(links))

            count = 0

            for link in links[:20]:

                try:

                    href = await link.get_attribute("href")

                    txt = await link.inner_text()

                    print("LINK =", txt[:80], "|", href)

                    count += 1

                except:
                    pass

        except Exception as e:

            print("❌ LINKS ERROR")
            print(e)

        # =====================================================
        # ARTICLES
        # =====================================================

        selectors = [

            "article",
            ".card",
            ".annonce",
            ".property",
            ".listing",
            ".bien"

        ]

        print("")
        print("🧩 SELECTORS")

        for sel in selectors:

            try:

                els = await page.query_selector_all(sel)

                print(sel, "=", len(els))

            except Exception as e:

                print(sel, "= ERROR")

        # =====================================================
        # RESULT
        # =====================================================

        if success and not forbidden:

            print("")
            print("🎉 SUCCESS POSSIBLE")
            print("=" * 80)

            return True

        print("")
        print("❌ FAILED")
        print("=" * 80)

        return False

    except Exception as e:

        print("")
        print("❌ EXCEPTION")
        print(e)

        traceback.print_exc()

        return False

    finally:

        try:

            if browser:
                await browser.close()

        except:
            pass


# =========================================================
# MAIN
# =========================================================

async def main():

    print("")
    print("=" * 80)
    print("🚀 IMMONOTAIRES EXTREME DEBUG")
    print("=" * 80)

    async with async_playwright() as p:

        # =====================================================
        # COMBOS
        # =====================================================

        combos = []

        for headless in [False, True]:

            for channel in [

                "chrome",
                None

            ]:

                for stealth in [

                    True,
                    False

                ]:

                    combos.append(
                        (
                            headless,
                            channel,
                            stealth
                        )
                    )

        # =====================================================
        # LOOP
        # =====================================================

        for url in URLS:

            for ua in USER_AGENTS:

                for combo in combos:

                    headless, channel, stealth = combo

                    ok = await test_combo(

                        playwright=p,

                        headless=headless,

                        channel=channel,

                        user_agent=ua,

                        use_stealth=stealth,

                        url=url

                    )

                    # =================================================
                    # STOP SI OK
                    # =================================================

                    if ok:

                        print("")
                        print("=" * 80)
                        print("🎯 SOLUTION TROUVEE")
                        print("=" * 80)

                        print("URL =", url)

                        print("HEADLESS =", headless)

                        print("CHANNEL =", channel)

                        print("STEALTH =", stealth)

                        print("UA =", ua)

                        print("=" * 80)

                        return

                    # =================================================
                    # ATTENTE
                    # =================================================

                    await asyncio.sleep(
                        random.randint(5, 15)
                    )

    print("")
    print("=" * 80)
    print("❌ AUCUNE SOLUTION")
    print("=" * 80)


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
