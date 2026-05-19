# =========================================================
# IMMO NOTAIRES ENCHERES
# VERSION ULTRA ROBUSTE DEBUG + MULTI SCRAP
# =========================================================

import asyncio
import pandas as pd
import re
import folium
import traceback
import os
import requests
import json

from playwright.async_api import async_playwright
from folium.features import DivIcon


# =========================================================
# CONFIG
# =========================================================

BASE_URL = "https://immonotairesencheres.com/bien"

CSV_CP = "base-officielle-codes-postaux.csv"

OUTPUT_MAP = "carte_immonotaires.html"

DEBUG_HTML = "debug_page.html"

HEADLESS = True

MAX_PAGES = 50


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram(message):

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("❌ TELEGRAM NON CONFIGURE")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    try:

        requests.post(
            url,
            data={
                "chat_id": chat_id,
                "text": message
            },
            timeout=20
        )

    except Exception as e:
        print(e)


def send_file(path):

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        return

    try:

        url = f"https://api.telegram.org/bot{token}/sendDocument"

        with open(path, "rb") as f:

            requests.post(
                url,
                data={"chat_id": chat_id},
                files={"document": f},
                timeout=60
            )

    except Exception as e:
        print(e)


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


def extract_surface(txt):

    try:

        matches = re.findall(
            r"(\d+(?:[.,]\d+)?)\s?m²",
            txt,
            re.I
        )

        vals = []

        for m in matches:

            vals.append(
                float(
                    m.replace(",", ".")
                )
            )

        if len(vals) == 0:
            return None

        return max(vals)

    except:
        return None


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


def extract_rooms(txt):

    try:

        m = re.search(
            r"(\d+)\s*pi[eè]ce",
            txt,
            re.I
        )

        if m:
            return int(m.group(1))

    except:
        pass

    return None


def detect_type(txt):

    t = txt.lower()

    if "appartement" in t:
        return "Appartement"

    if "studio" in t:
        return "Appartement"

    if "maison" in t:
        return "Maison"

    if "villa" in t:
        return "Villa"

    if "terrain" in t:
        return "Terrain"

    if "immeuble" in t:
        return "Immeuble"

    if "local commercial" in t:
        return "Local commercial"

    return "Autre"


# =========================================================
# GEOLOCALISATION
# =========================================================

def geolocate(df):

    geo = pd.read_csv(CSV_CP)

    geo = geo[[
        "code_postal",
        "latitude",
        "longitude"
    ]]

    geo.columns = [
        "cp",
        "lat",
        "lon"
    ]

    geo["cp"] = geo["cp"].astype(str)

    df["cp"] = (
        df["cp"]
        .fillna("")
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )

    df = df.merge(
        geo,
        on="cp",
        how="left"
    )

    print(
        "📍 GEOLOCALISATION OK :",
        df["lat"].notna().sum(),
        "annonces"
    )

    return df


# =========================================================
# DEBUG
# =========================================================

async def debug_page(page):

    try:

        html = await page.content()

        with open(
            DEBUG_HTML,
            "w",
            encoding="utf-8"
        ) as f:

            f.write(html)

        print("💾 DEBUG HTML SAUVEGARDE")

        print("")
        print("=" * 80)
        print("DEBUG HTML")
        print("=" * 80)

        print(html[:5000])

        print("=" * 80)

    except Exception as e:

        print("❌ DEBUG")
        print(e)


# =========================================================
# MULTI SCRAP
# =========================================================

async def extract_method_1(page):

    print("")
    print("🔍 METHODE 1 : article.node-property")

    return await page.query_selector_all(
        "article.node-property"
    )


async def extract_method_2(page):

    print("")
    print("🔍 METHODE 2 : article")

    return await page.query_selector_all(
        "article"
    )


async def extract_method_3(page):

    print("")
    print("🔍 METHODE 3 : liens /bien/")

    return await page.query_selector_all(
        "a[href*='/bien/']"
    )


async def extract_method_4(page):

    print("")
    print("🔍 METHODE 4 : cards")

    selectors = [
        ".card",
        ".property",
        ".property-card",
        ".bien",
        ".listing",
        ".annonce"
    ]

    all_elements = []

    for sel in selectors:

        try:

            els = await page.query_selector_all(sel)

            print(sel, "=", len(els))

            all_elements.extend(els)

        except:
            pass

    return all_elements


# =========================================================
# SCRAPE
# =========================================================

async def scrape():

    rows = []

    seen = set()

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=HEADLESS
        )

        page = await browser.new_page()

        await page.set_viewport_size({
            "width": 1600,
            "height": 3000
        })

        for page_num in range(MAX_PAGES):

            print("")
            print("=" * 80)
            print(f"📄 PAGE {page_num}")
            print("=" * 80)

            url = f"{BASE_URL}?page={page_num}"

            print(url)

            try:

                response = await page.goto(
                    url,
                    timeout=60000,
                    wait_until="domcontentloaded"
                )

                print(
                    "🌐 STATUS =",
                    response.status if response else "?"
                )

            except Exception as e:

                print("❌ GOTO")
                print(e)

                break

            try:

                await page.wait_for_load_state(
                    "networkidle",
                    timeout=30000
                )

            except:
                pass

            await page.wait_for_timeout(5000)

            # scroll forcé

            try:

                for _ in range(10):

                    await page.mouse.wheel(0, 5000)

                    await page.wait_for_timeout(1000)

            except:
                pass

            # DEBUG HTML

            await debug_page(page)

            # TESTS SELECTEURS

            methods = []

            m1 = await extract_method_1(page)
            methods.extend(m1)

            m2 = await extract_method_2(page)
            methods.extend(m2)

            m3 = await extract_method_3(page)
            methods.extend(m3)

            m4 = await extract_method_4(page)
            methods.extend(m4)

            print("")
            print("🧩 ELEMENTS TROUVES =", len(methods))

            if len(methods) == 0:

                print("❌ AUCUN ELEMENT")

                screenshot = f"screenshot_{page_num}.png"

                await page.screenshot(
                    path=screenshot,
                    full_page=True
                )

                print("📸 SCREENSHOT =", screenshot)

                break

            before = len(rows)

            for el in methods:

                try:

                    txt = clean(
                        await el.inner_text()
                    )

                    href = None

                    # lien direct

                    try:

                        href = await el.get_attribute(
                            "href"
                        )

                    except:
                        pass

                    # lien enfant

                    if not href:

                        try:

                            link = await el.query_selector(
                                "a"
                            )

                            if link:

                                href = await link.get_attribute(
                                    "href"
                                )

                        except:
                            pass

                    if not href:
                        continue

                    if "/bien/" not in href:
                        continue

                    if href.startswith("/"):

                        annonce_url = (
                            "https://immonotairesencheres.com"
                            + href
                        )

                    else:

                        annonce_url = href

                    if annonce_url in seen:
                        continue

                    seen.add(annonce_url)

                    row = {

                        "url": annonce_url,

                        "txt": txt,

                        "prix": extract_price(txt),

                        "surface": extract_surface(txt),

                        "cp": extract_cp(txt),

                        "pieces": extract_rooms(txt),

                        "type": detect_type(txt)

                    }

                    rows.append(row)

                    print(
                        f"✅ {row['type']} | "
                        f"{row['prix']}€ | "
                        f"{row['cp']}"
                    )

                except Exception as e:

                    print("❌ ELEMENT")
                    print(e)

            added = len(rows) - before

            print("")
            print("➕ AJOUTES =", added)

            if added == 0:

                print("⛔ FIN PAGINATION")

                break

        await browser.close()

    df = pd.DataFrame(rows)

    if len(df) > 0:

        df = df.drop_duplicates(
            subset="url"
        )

    return df


# =========================================================
# MAP
# =========================================================

def create_map(df):

    m = folium.Map(
        location=[46.5, 2.5],
        zoom_start=6,
        tiles="CartoDB positron"
    )

    for _, row in df.iterrows():

        try:

            if pd.isna(row["lat"]):
                continue

            popup = f"""
<b>{row['type']}</b><br><br>

💰 {row['prix']} €<br>

📐 {row['surface']} m²<br>

🚪 {row['pieces']} pièces<br>

📍 {row['cp']}<br><br>

<a href="{row['url']}" target="_blank">
Voir annonce
</a>
"""

            folium.Marker(

                location=[
                    row["lat"],
                    row["lon"]
                ],

                popup=popup

            ).add_to(m)

        except Exception as e:

            print(e)

    m.save(OUTPUT_MAP)

    print("✅ CARTE SAUVEGARDEE")


# =========================================================
# MAIN
# =========================================================

async def main():

    try:

        print("")
        print("=" * 80)
        print("🚀 DEMARRAGE")
        print("=" * 80)

        df = await scrape()

        print("")
        print("=" * 80)
        print(f"📦 TOTAL = {len(df)}")
        print("=" * 80)

        if len(df) == 0:

            print("❌ AUCUNE ANNONCE")

            send_telegram(
                "❌ IMMO NOTAIRES\nAUCUNE ANNONCE"
            )

            return

        print("")
        print(df.head())

        df.to_csv(
            "immonotaires.csv",
            sep=";",
            index=False,
            encoding="utf-8-sig"
        )

        print("💾 CSV SAUVEGARDE")

        df = geolocate(df)

        create_map(df)

        send_file(OUTPUT_MAP)

        send_file("immonotaires.csv")

        send_telegram(
            f"✅ IMMO NOTAIRES\n"
            f"{len(df)} annonces"
        )

        print("✅ FIN")

    except Exception as e:

        print("")
        print("=" * 80)
        print("❌ ERREUR MAIN")
        print("=" * 80)

        print(e)

        traceback.print_exc()

        send_telegram(
            f"❌ ERREUR\n{e}"
        )


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
