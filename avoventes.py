# ============================================================
# AVOVENTES SCRAPER ULTRA DEBUG - VERSION AUTONOME
# ============================================================
# Compatible GitHub Actions sans bs4
#
# Dépendances minimales :
# pip install playwright pandas
# playwright install chromium
#
# ============================================================

import asyncio
import json
import re
from urllib.parse import urljoin

import pandas as pd
from playwright.async_api import async_playwright


URL = "https://avoventes.fr/recherche/toutes"


# ============================================================
# HELPERS
# ============================================================

def sep(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def save_txt(filename, content):

    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)


def save_json(filename, data):

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ============================================================
# MAIN
# ============================================================

async def main():

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage"
            ]
        )

        context = await browser.new_context(
            viewport={"width": 1600, "height": 4000},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )

        page = await context.new_page()

        # ====================================================
        # CAPTURE RESEAUX
        # ====================================================

        api_calls = []

        async def handle_response(response):

            try:

                url = response.url.lower()

                ct = response.headers.get(
                    "content-type",
                    ""
                ).lower()

                interesting = any([
                    "json" in ct,
                    "api" in url,
                    "search" in url,
                    "recherche" in url,
                    "vente" in url,
                    "encher" in url,
                    "annonce" in url,
                ])

                if interesting:

                    print("\n📡 RESPONSE")
                    print("URL =", response.url)
                    print("STATUS =", response.status)
                    print("CONTENT TYPE =", ct)

                    api_calls.append({
                        "url": response.url,
                        "status": response.status,
                        "content_type": ct
                    })

                    try:

                        txt = await response.text()

                        filename = (
                            "api_" +
                            re.sub(r"[^a-zA-Z0-9]", "_", response.url)[:80] +
                            ".txt"
                        )

                        save_txt(
                            filename,
                            txt[:200000]
                        )

                    except Exception as e:
                        print("response text error", e)

            except Exception as e:
                print("handle_response error", e)

        page.on("response", handle_response)

        # ====================================================
        # GOTO
        # ====================================================

        sep("OPEN PAGE")

        await page.goto(
            URL,
            wait_until="networkidle",
            timeout=120000
        )

        print("FINAL URL =", page.url)

        title = await page.title()

        print("TITLE =", title)

        # ====================================================
        # ATTENTE LONGUE
        # ====================================================

        sep("WAIT")

        await page.wait_for_timeout(15000)

        # ====================================================
        # COOKIES
        # ====================================================

        sep("TRY ACCEPT COOKIES")

        cookie_selectors = [
            "button:has-text('Accepter')",
            "button:has-text('Tout accepter')",
            "button:has-text('OK')",
            "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
            ".CybotCookiebotDialogBodyButton",
        ]

        for sel in cookie_selectors:

            try:

                locator = page.locator(sel)

                count = await locator.count()

                print(sel, "->", count)

                if count > 0:

                    await locator.first.click(timeout=3000)

                    print("COOKIE CLICKED =", sel)

                    await page.wait_for_timeout(5000)

                    break

            except Exception as e:
                print("cookie error", sel, e)

        # ====================================================
        # SCREENSHOT INITIAL
        # ====================================================

        sep("SCREENSHOT")

        await page.screenshot(
            path="debug_initial.png",
            full_page=True
        )

        print("saved debug_initial.png")

        # ====================================================
        # HTML INITIAL
        # ====================================================

        sep("HTML")

        html = await page.content()

        save_txt(
            "debug_initial.html",
            html
        )

        print("HTML SIZE =", len(html))

        # ====================================================
        # BODY TEXT
        # ====================================================

        sep("BODY TEXT")

        body_text = await page.locator("body").inner_text()

        save_txt(
            "body_text.txt",
            body_text
        )

        print(body_text[:5000])

        # ====================================================
        # IFRAME DEBUG
        # ====================================================

        sep("IFRAMES")

        frames = page.frames

        print("FRAME COUNT =", len(frames))

        for i, frame in enumerate(frames):

            print("\nFRAME", i)
            print("URL =", frame.url)

            try:

                frame_html = await frame.content()

                save_txt(
                    f"frame_{i}.html",
                    frame_html
                )

                print("SIZE =", len(frame_html))

            except Exception as e:
                print("frame error", e)

        # ====================================================
        # LOCATORS DEBUG
        # ====================================================

        sep("LOCATOR COUNTS")

        selectors = [
            "article",
            ".card",
            ".vente",
            ".annonce",
            ".listing",
            ".item",
            ".result",
            "a",
            "img",
            "[class*=vente]",
            "[class*=annonce]",
            "[class*=card]",
            "[class*=result]",
        ]

        locator_counts = []

        for sel in selectors:

            try:

                count = await page.locator(sel).count()

                locator_counts.append({
                    "selector": sel,
                    "count": count
                })

                print(f"{sel:30} -> {count}")

            except Exception as e:
                print(sel, e)

        save_json(
            "locator_counts.json",
            locator_counts
        )

        # ====================================================
        # TOUS LES LIENS
        # ====================================================

        sep("ALL LINKS")

        links = await page.locator("a").evaluate_all("""
        els => els.map(e => ({
            href: e.href || "",
            text: e.innerText || "",
            className: e.className || ""
        }))
        """)

        print("TOTAL LINKS =", len(links))

        save_json(
            "all_links.json",
            links
        )

        # ====================================================
        # FILTRE ANNONCES
        # ====================================================

        sep("POTENTIAL ANNOUNCES")

        keywords = [
            "vente",
            "encher",
            "bien",
            "maison",
            "appartement",
            "villa",
            "terrain",
            "immeuble",
            "local",
            "adjudication",
        ]

        banned = [
            "facebook",
            "linkedin",
            "twitter",
            "youtube",
            "cookie",
            "privacy",
            "login",
            "connexion",
            "contact",
        ]

        annonces = []

        for l in links:

            href = l["href"].lower()
            text = l["text"].lower()

            if any(b in href for b in banned):
                continue

            if any(b in text for b in banned):
                continue

            good = False

            if any(k in href for k in keywords):
                good = True

            if any(k in text for k in keywords):
                good = True

            if good:

                annonces.append(l)

                print("\n✅ POSSIBLE")
                print("TEXT =", l["text"])
                print("URL =", l["href"])

        print("\nTOTAL POSSIBLE =", len(annonces))

        save_json(
            "potential_annonces.json",
            annonces
        )

        # ====================================================
        # SCROLL MASSIF
        # ====================================================

        sep("SCROLL")

        previous_height = 0

        for i in range(25):

            height = await page.evaluate(
                "document.body.scrollHeight"
            )

            print(f"SCROLL {i+1}/25 HEIGHT =", height)

            if height == previous_height:
                print("HEIGHT STABLE")

            previous_height = height

            await page.mouse.wheel(0, 8000)

            await page.wait_for_timeout(2500)

        # ====================================================
        # SCREENSHOT FINAL
        # ====================================================

        sep("FINAL SCREENSHOT")

        await page.screenshot(
            path="debug_after_scroll.png",
            full_page=True
        )

        # ====================================================
        # HTML FINAL
        # ====================================================

        sep("FINAL HTML")

        html2 = await page.content()

        save_txt(
            "debug_after_scroll.html",
            html2
        )

        print("FINAL HTML SIZE =", len(html2))

        # ====================================================
        # LINKS AFTER SCROLL
        # ====================================================

        sep("LINKS AFTER SCROLL")

        links2 = await page.locator("a").evaluate_all("""
        els => els.map(e => ({
            href: e.href || "",
            text: e.innerText || "",
            className: e.className || ""
        }))
        """)

        print("TOTAL LINKS AFTER SCROLL =", len(links2))

        save_json(
            "all_links_after_scroll.json",
            links2
        )

        # ====================================================
        # PATTERNS IMMOBILIERS
        # ====================================================

        sep("IMMOBILIER PATTERNS")

        patterns = [
            r"\d+\s?€",
            r"mise à prix",
            r"adjudication",
            r"appartement",
            r"maison",
            r"villa",
            r"terrain",
            r"m²",
        ]

        pattern_results = []

        final_text = await page.locator("body").inner_text()

        for ptn in patterns:

            found = re.findall(
                ptn,
                final_text,
                flags=re.I
            )

            print("\nPATTERN =", ptn)
            print("COUNT =", len(found))

            pattern_results.append({
                "pattern": ptn,
                "count": len(found),
                "examples": found[:20]
            })

        save_json(
            "patterns.json",
            pattern_results
        )

        # ====================================================
        # EXPORT CSV
        # ====================================================

        sep("CSV EXPORT")

        df = pd.DataFrame(annonces)

        df.to_csv(
            "potential_annonces.csv",
            index=False
        )

        print(df.head())

        # ====================================================
        # API EXPORT
        # ====================================================

        save_json(
            "api_calls.json",
            api_calls
        )

        print("\nAPI CALLS =", len(api_calls))

        # ====================================================
        # DONE
        # ====================================================

        sep("DONE")

        print("""
FILES GENERATED:

- debug_initial.png
- debug_initial.html
- debug_after_scroll.png
- debug_after_scroll.html
- body_text.txt
- frame_*.html
- all_links.json
- all_links_after_scroll.json
- potential_annonces.json
- potential_annonces.csv
- api_calls.json
- locator_counts.json
- patterns.json
""")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
