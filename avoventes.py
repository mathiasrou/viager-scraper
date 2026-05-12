# =========================================================
# AVOVENTES.FR
# VERSION DEBUG COMPLETE
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

BASE_URL = "https://avoventes.fr/recherche/toutes"

CSV_CP = "base-officielle-codes-postaux.csv"

HISTORY_FILE = "historique_avoventes.csv"

OUTPUT_MAP = "carte_avoventes.html"


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

    requests.post(
        url,
        data={
            "chat_id": chat_id,
            "text": message
        }
    )


def send_file(path):

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        return

    url = f"https://api.telegram.org/bot{token}/sendDocument"

    with open(path, "rb") as f:

        requests.post(
            url,
            data={"chat_id": chat_id},
            files={"document": f}
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

    txt = re.sub(r"\s+", " ", txt)

    return txt.strip()


# =========================================================
# EXTRACTIONS
# =========================================================

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

    return "Autre"


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
            r"(\d+(?:[.,]\d+)?)\s?(?:m²|m2)",
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
    )

    df = df.merge(
        geo,
        on="cp",
        how="left"
    )

    print("")
    print("================================================")
    print("📍 GEOLOCALISATION")
    print("================================================")

    print(
        f"📍 {df['lat'].notna().sum()} annonces geolocalisées"
    )

    return df


# =========================================================
# SCRAPE
# =========================================================

async def scrape():

    rows = []

    seen = set()

    api_urls = []

    print("")
    print("================================================")
    print("🚀 DEBUT SCRAPE AVOVENTES")
    print("================================================")

    async with async_playwright() as p:

        browser = await p.chromium.launch(

            headless=True,

            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled"
            ]
        )

        context = await browser.new_context(

            user_agent=(
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),

            viewport={
                "width": 1600,
                "height": 900
            },

            locale="fr-FR"
        )

        page = await context.new_page()

        # =====================================================
        # DEBUG REQUESTS
        # =====================================================

        async def handle_response(response):

            try:

                url = response.url

                if (
                    "api" in url.lower()
                    or "json" in url.lower()
                    or "search" in url.lower()
                ):

                    print("")
                    print("📡 API / JSON")
                    print(url)

                    api_urls.append(url)

            except:
                pass

        page.on(
            "response",
            handle_response
        )

        # =====================================================
        # OPEN PAGE
        # =====================================================

        print("")
        print("================================================")
        print("🌐 OUVERTURE URL")
        print("================================================")

        print(BASE_URL)

        await page.goto(
            BASE_URL,
            wait_until="domcontentloaded",
            timeout=120000
        )

        # =====================================================
        # COOKIES
        # =====================================================

        print("")
        print("================================================")
        print("🍪 COOKIES")
        print("================================================")

        try:

            await page.click(
                'button:has-text("Accepter")',
                timeout=10000
            )

            print("✅ COOKIES ACCEPTES")

        except Exception as e:

            print("⚠️ PAS DE POPUP COOKIE")

        # =====================================================
        # WAIT
        # =====================================================

        print("")
        print("================================================")
        print("⏳ ATTENTE")
        print("================================================")

        await page.wait_for_timeout(10000)

        # =====================================================
        # SCROLL
        # =====================================================

        print("")
        print("================================================")
        print("🖱️ SCROLL")
        print("================================================")

        for i in range(10):

            print(f"🖱️ SCROLL {i+1}/10")

            await page.mouse.wheel(0, 3000)

            await page.wait_for_timeout(2500)

        # =====================================================
        # SCREENSHOT
        # =====================================================

        print("")
        print("================================================")
        print("🖼️ SCREENSHOT")
        print("================================================")

        await page.screenshot(
            path="debug_avoventes.png",
            full_page=True
        )

        print("✅ SCREENSHOT SAUVEGARDE")

        # =====================================================
        # HTML
        # =====================================================

        print("")
        print("================================================")
        print("💾 HTML")
        print("================================================")

        html = await page.content()

        with open(
            "debug_avoventes.html",
            "w",
            encoding="utf-8"
        ) as f:

            f.write(html)

        print("✅ HTML SAUVEGARDE")

        # =====================================================
        # JSON
        # =====================================================

        with open(
            "debug_api.json",
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                api_urls,
                f,
                indent=2,
                ensure_ascii=False
            )

        # =====================================================
        # DEBUG SELECTORS
        # =====================================================

        print("")
        print("================================================")
        print("🔎 SELECTORS")
        print("================================================")

        selectors = [
            "article",
            ".card",
            ".annonce",
            ".item",
            "a"
        ]

        for sel in selectors:

            try:

                n = await page.locator(sel).count()

                print(f"{sel} => {n}")

            except Exception as e:

                print(sel)
                print(e)

        # =====================================================
        # LINKS
        # =====================================================

        print("")
        print("================================================")
        print("🔗 EXTRACTION URLS")
        print("================================================")

        links = await page.locator("a").evaluate_all(
            """
            els => els.map(e => e.href)
            """
        )

        print(f"🔗 TOTAL LINKS = {len(links)}")

        for link in links:

            try:

                if "/vente/" not in link:
                    continue

                if link in seen:
                    continue

                seen.add(link)

                txt = clean(link)

                row = {

                    "url": link,

                    "txt": txt,

                    "prix": extract_price(txt),

                    "surface": extract_surface(txt),

                    "cp": extract_cp(txt),

                    "type": "Autre"

                }

                rows.append(row)

                print("")
                print("✅ URL")
                print(link)

            except Exception as e:

                print(e)

        await browser.close()

    df = pd.DataFrame(rows)

    print("")
    print("================================================")
    print("📦 RESULTAT FINAL")
    print("================================================")

    print(f"📦 TOTAL = {len(df)}")

    return df


# =========================================================
# MAP
# =========================================================

def create_map(df):

    m = folium.Map(
        location=[46.5, 2.5],
        zoom_start=6
    )

    for _, row in df.iterrows():

        try:

            if pd.isna(row["lat"]):
                continue

            folium.Marker(

                [
                    row["lat"],
                    row["lon"]
                ],

                popup=row["url"]

            ).add_to(m)

        except:
            pass

    m.save(OUTPUT_MAP)

    print("✅ CARTE SAUVEGARDEE")


# =========================================================
# MAIN
# =========================================================

async def main():

    try:

        df = await scrape()

        if len(df) == 0:

            print("")
            print("================================================")
            print("❌ AUCUNE ANNONCE")
            print("================================================")

            send_telegram(
                "❌ AVOVENTES : aucune annonce"
            )

            return

        df.to_csv(
            HISTORY_FILE,
            sep=";",
            index=False,
            encoding="utf-8-sig"
        )

        print("✅ CSV SAUVEGARDE")

        df = geolocate(df)

        create_map(df)

        send_file(OUTPUT_MAP)

        send_telegram(
            f"✅ AVOVENTES\n{len(df)} annonces"
        )

    except Exception as e:

        print("")
        print("================================================")
        print("❌ ERREUR MAIN")
        print("================================================")

        print(e)

        traceback.print_exc()

        send_telegram(
            f"❌ ERREUR AVOVENTES\n{e}"
        )


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
