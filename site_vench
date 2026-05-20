```python
# =========================================================
# VENCH SCRAPER DEBUG ULTRA VERBEUX
# =========================================================

import asyncio
import pandas as pd
import re
import os
import traceback
import requests
import folium

from playwright.async_api import async_playwright

# =========================================================
# CONFIG
# =========================================================

BASE_URL = "https://www.vench.fr/prochaines-ventes-aux-encheres.html"

OUTPUT_DEBUG_DIR = "debug_vench"

os.makedirs(
    OUTPUT_DEBUG_DIR,
    exist_ok=True
)

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
# SCRAPE
# =========================================================

async def scrape():

    rows = []

    seen = set()

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=False,
            slow_mo=300
        )

        page = await browser.new_page(
            viewport={
                "width": 1600,
                "height": 4000
            }
        )

        detail = await browser.new_page()

        page_num = 1

        while True:

            print("")
            print("=" * 80)
            print(f"📄 PAGE {page_num}")
            print("=" * 80)

            if page_num == 1:

                url = BASE_URL

            else:

                url = (
                    BASE_URL
                    + f"?p={page_num}"
                )

            print(f"🌐 OPEN : {url}")

            await page.goto(
                url,
                wait_until="networkidle",
                timeout=120000
            )

            await page.wait_for_timeout(5000)

            # =====================================================
            # DEBUG PAGE LISTE
            # =====================================================

            html = await page.content()

            with open(
                f"{OUTPUT_DEBUG_DIR}/liste_{page_num}.html",
                "w",
                encoding="utf-8"
            ) as f:

                f.write(html)

            txt = await page.locator("body").inner_text()

            with open(
                f"{OUTPUT_DEBUG_DIR}/liste_{page_num}.txt",
                "w",
                encoding="utf-8"
            ) as f:

                f.write(txt)

            await page.screenshot(
                path=f"{OUTPUT_DEBUG_DIR}/liste_{page_num}.png",
                full_page=True
            )

            # =====================================================
            # LIENS
            # =====================================================

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

            print(f"🔗 NB LINKS = {len(links)}")

            annonce_urls = []

            for l in links:

                href = l.get("href")

                if href is None:
                    continue

                if "/vente-" in href:

                    if href not in annonce_urls:

                        annonce_urls.append(href)

            print(f"🏠 ANNONCES = {len(annonce_urls)}")

            if len(annonce_urls) == 0:

                print("❌ AUCUNE ANNONCE")
                break

            # =====================================================
            # DETAILS
            # =====================================================

            for i, annonce_url in enumerate(annonce_urls):

                try:

                    if annonce_url in seen:
                        continue

                    seen.add(annonce_url)

                    print("")
                    print("-" * 60)
                    print(f"🏠 {i+1}/{len(annonce_urls)}")
                    print(annonce_url)

                    await detail.goto(
                        annonce_url,
                        wait_until="networkidle",
                        timeout=120000
                    )

                    await detail.wait_for_timeout(3000)

                    detail_txt = await detail.locator(
                        "body"
                    ).inner_text()

                    detail_txt = clean(detail_txt)

                    print("")
                    print("📄 TEXTE DETAIL")
                    print(detail_txt[:3000])

                    # =============================================
                    # DEBUG DETAIL
                    # =============================================

                    safe = str(i).zfill(3)

                    with open(
                        f"{OUTPUT_DEBUG_DIR}/detail_{page_num}_{safe}.txt",
                        "w",
                        encoding="utf-8"
                    ) as f:

                        f.write(detail_txt)

                    html_detail = await detail.content()

                    with open(
                        f"{OUTPUT_DEBUG_DIR}/detail_{page_num}_{safe}.html",
                        "w",
                        encoding="utf-8"
                    ) as f:

                        f.write(html_detail)

                    await detail.screenshot(
                        path=f"{OUTPUT_DEBUG_DIR}/detail_{page_num}_{safe}.png",
                        full_page=True
                    )

                    # =============================================
                    # EXTRACTIONS
                    # =============================================

                    row = {}

                    row["url"] = annonce_url

                    row["txt"] = detail_txt

                    row["cp"] = extract_cp(detail_txt)

                    row["prix"] = extract_price(detail_txt)

                    row["type"] = detect_type(detail_txt)

                    row["titre"] = clean(
                        detail_txt[:200]
                    )

                    rows.append(row)

                    print("")
                    print("✅ EXTRACTION")
                    print(row)

                except Exception as e:

                    print("")
                    print("❌ ERREUR DETAIL")
                    print(e)

                    traceback.print_exc()

            page_num += 1

            if page_num > 5:
                break

        await browser.close()

    df = pd.DataFrame(rows)

    return df

# =========================================================
# MAIN
# =========================================================

async def main():

    df = await scrape()

    print("")
    print("=" * 80)
    print("📊 RESULTAT")
    print("=" * 80)

    print(df.head())

    df.to_csv(
        "vench_debug.csv",
        sep=";",
        index=False,
        encoding="utf-8-sig"
    )

    print("")
    print("💾 CSV SAUVE")

asyncio.run(main())
```
