# ============================================================
# SCRAPER AVOVENTES ULTRA DEBUG
# ============================================================
# Objectif :
# - comprendre EXACTEMENT où sont les annonces
# - détecter iframe / JS / API / lazy loading
# - sauvegarder énormément de debug
# - tenter plusieurs stratégies automatiquement
#
# Nécessite :
# pip install playwright bs4 pandas lxml
# playwright install
# ============================================================

import asyncio
import json
import re
import time
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


URL = "https://avoventes.fr/recherche/toutes"


# ============================================================
# HELPERS
# ============================================================

def save(name, content, mode="w", encoding="utf-8"):
    with open(name, mode, encoding=encoding) as f:
        f.write(content)


def sep(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


# ============================================================
# MAIN
# ============================================================

async def main():

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=False,
            slow_mo=250
        )

        context = await browser.new_context(
            viewport={"width": 1600, "height": 2400}
        )

        page = await context.new_page()

        # ====================================================
        # CAPTURE TOUS LES XHR / FETCH / API
        # ====================================================

        api_calls = []

        async def handle_response(response):

            try:

                url = response.url
                ct = response.headers.get("content-type", "")

                interesting = any([
                    "json" in ct,
                    "api" in url.lower(),
                    "vente" in url.lower(),
                    "encher" in url.lower(),
                    "search" in url.lower(),
                    "recherche" in url.lower(),
                ])

                if interesting:

                    print("\n📡 API/RESPONSE")
                    print(url)
                    print(ct)

                    api_calls.append({
                        "url": url,
                        "content_type": ct,
                        "status": response.status
                    })

                    try:

                        txt = await response.text()

                        if len(txt) > 100000:
                            txt = txt[:100000]

                        filename = (
                            "api_" +
                            re.sub(r"[^a-zA-Z0-9]", "_", url)[:120] +
                            ".txt"
                        )

                        save(filename, txt)

                    except Exception as e:
                        print("response text error", e)

            except Exception as e:
                print("handle_response error", e)

        page.on("response", handle_response)

        # ====================================================
        # GOTO
        # ====================================================

        sep("GOTO")

        await page.goto(
            URL,
            wait_until="domcontentloaded",
            timeout=120000
        )

        print("URL =", page.url)
        print("TITLE =", await page.title())

        # ====================================================
        # ATTENTE LONGUE
        # ====================================================

        sep("LONG WAIT")

        for i in range(30):

            await page.wait_for_timeout(1000)

            print(f"wait {i+1}/30")

        # ====================================================
        # SCREENSHOT
        # ====================================================

        sep("SCREENSHOT")

        await page.screenshot(
            path="debug_full.png",
            full_page=True
        )

        print("saved screenshot")

        # ====================================================
        # HTML
        # ====================================================

        sep("HTML")

        html = await page.content()

        save("debug_page.html", html)

        print("html size =", len(html))

        # ====================================================
        # INNER TEXT
        # ====================================================

        sep("BODY TEXT")

        try:

            body_text = await page.locator("body").inner_text()

            save("body_text.txt", body_text)

            print(body_text[:5000])

        except Exception as e:
            print("body text error", e)

        # ====================================================
        # IFRAMES
        # ====================================================

        sep("IFRAMES")

        frames = page.frames

        print("frame count =", len(frames))

        for i, frame in enumerate(frames):

            print("\nFRAME", i)
            print("url =", frame.url)

            try:

                frame_html = await frame.content()

                save(f"frame_{i}.html", frame_html)

                print("html size =", len(frame_html))

            except Exception as e:
                print("frame content error", e)

        # ====================================================
        # LOCATORS MASSIFS
        # ====================================================

        sep("LOCATOR COUNTS")

        selectors = [
            "article",
            ".card",
            ".item",
            ".listing",
            ".vente",
            ".annonce",
            ".property",
            ".result",
            ".search-result",
            "[class*=vente]",
            "[class*=annonce]",
            "[class*=card]",
            "[class*=item]",
            "[class*=result]",
            "a",
            "img",
        ]

        for sel in selectors:

            try:

                count = await page.locator(sel).count()

                print(f"{sel:30} -> {count}")

            except Exception as e:
                print(sel, e)

        # ====================================================
        # TOUS LES LIENS
        # ====================================================

        sep("ALL LINKS")

        links = await page.locator("a").evaluate_all("""
        els => els.map(e => ({
            href: e.href,
            text: e.innerText,
            cls: e.className
        }))
        """)

        save(
            "all_links.json",
            json.dumps(links, indent=2, ensure_ascii=False)
        )

        print("link count =", len(links))

        # ====================================================
        # FILTRE DES LIENS SUSPECTS
        # ====================================================

        sep("POTENTIAL ANNOUNCES")

        potential = []

        keywords = [
            "vente",
            "encher",
            "bien",
            "maison",
            "appartement",
            "villa",
            "terrain",
            "local",
            "immeuble",
        ]

        for l in links:

            txt = (l.get("text") or "").lower()
            href = (l.get("href") or "").lower()

            if any(k in txt for k in keywords) or any(k in href for k in keywords):

                potential.append(l)

                print("\nTEXT =", l["text"])
                print("HREF =", l["href"])

        save(
            "potential_links.json",
            json.dumps(potential, indent=2, ensure_ascii=False)
        )

        print("\npotential =", len(potential))

        # ====================================================
        # SCROLL INFINI
        # ====================================================

        sep("SCROLL TEST")

        previous_height = 0

        for i in range(20):

            current_height = await page.evaluate(
                "document.body.scrollHeight"
            )

            print("height =", current_height)

            if current_height == previous_height:
                print("height stable")
            else:
                previous_height = current_height

            await page.mouse.wheel(0, 5000)

            await page.wait_for_timeout(2000)

        await page.screenshot(
            path="after_scroll.png",
            full_page=True
        )

        # ====================================================
        # HTML APRES SCROLL
        # ====================================================

        sep("HTML AFTER SCROLL")

        html2 = await page.content()

        save("after_scroll.html", html2)

        print("html2 size =", len(html2))

        # ====================================================
        # RECHECK COUNTS
        # ====================================================

        sep("COUNTS AFTER SCROLL")

        for sel in selectors:

            try:

                count = await page.locator(sel).count()

                print(f"{sel:30} -> {count}")

            except Exception as e:
                print(sel, e)

        # ====================================================
        # NETWORK DEBUG
        # ====================================================

        sep("API CALLS")

        print("api calls =", len(api_calls))

        save(
            "api_calls.json",
            json.dumps(api_calls, indent=2)
        )

        for a in api_calls:
            print(a)

        # ====================================================
        # BEAUTIFULSOUP ANALYSIS
        # ====================================================

        sep("BS4 ANALYSIS")

        soup = BeautifulSoup(html2, "lxml")

        all_divs = soup.find_all("div")

        print("div count =", len(all_divs))

        classes = {}

        for d in all_divs:

            cls = d.get("class")

            if cls:

                for c in cls:

                    classes[c] = classes.get(c, 0) + 1

        sorted_classes = sorted(
            classes.items(),
            key=lambda x: x[1],
            reverse=True
        )

        save(
            "classes.json",
            json.dumps(sorted_classes, indent=2)
        )

        print("\nTOP CLASSES")

        for c, n in sorted_classes[:200]:
            print(n, c)

        # ====================================================
        # DETECTION TEXTE IMMOBILIER
        # ====================================================

        sep("IMMOBILIER DETECTION")

        immobilier_patterns = [
            r"\d+\s?€",
            r"mise à prix",
            r"adjudication",
            r"appartement",
            r"maison",
            r"villa",
            r"terrain",
            r"local commercial",
            r"immeuble",
            r"m²",
        ]

        for pat in immobilier_patterns:

            found = re.findall(
                pat,
                body_text,
                flags=re.I
            )

            print("\nPATTERN =", pat)
            print("COUNT =", len(found))

            if found:
                print(found[:20])

        # ====================================================
        # EXPORT DATAFRAME
        # ====================================================

        sep("DATAFRAME")

        df = pd.DataFrame(potential)

        print(df.head())

        df.to_csv(
            "potential_annonces.csv",
            index=False
        )

        # ====================================================
        # FIN
        # ====================================================

        sep("DONE")

        print("""
FILES GENERATED:
- debug_full.png
- debug_page.html
- body_text.txt
- frame_*.html
- all_links.json
- potential_links.json
- after_scroll.png
- after_scroll.html
- api_calls.json
- classes.json
- potential_annonces.csv
""")

        await browser.close()


asyncio.run(main())
