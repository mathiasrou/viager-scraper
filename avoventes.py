# =========================================================
# AVOVENTES SCRAPER
# VERSION COMPLETE GITHUB
# =========================================================

import asyncio
import pandas as pd
import re
import folium
import traceback
import os
import requests

from playwright.async_api import async_playwright
from folium.features import DivIcon


# =========================================================
# CONFIG
# =========================================================

BASE_URL = "https://avoventes.fr/recherche/toutes"

CSV_CP = "base-officielle-codes-postaux.csv"

OUTPUT_MAP = "carte_avoventes.html"

OUTPUT_CSV = "historique_avoventes.csv"


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram(msg):

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    requests.post(
        url,
        data={
            "chat_id": chat_id,
            "text": msg
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
            data={
                "chat_id": chat_id
            },
            files={
                "document": f
            }
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

    txt = re.sub(
        r"\s+",
        " ",
        txt
    )

    return txt.strip()


# =========================================================
# TYPE
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

    if "atelier" in t:
        return "Atelier"

    return "Autre"


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

            m = re.sub(
                r"\s+",
                "",
                m
            )

            if m.isdigit():

                v = int(m)

                if 1000 <= v <= 100000000:
                    vals.append(v)

        if len(vals) == 0:
            return None

        return min(vals)

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
# GEO
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

    geo["cp"] = (
        geo["cp"]
        .astype(str)
        .str.strip()
    )

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

    print("")
    print("================================================")
    print("📍 GEOLOCALISATION")
    print("================================================")

    print(
        f"📍 GEO OK = {df['lat'].notna().sum()}"
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

    css = """
<style>
.leaflet-div-icon{
    background:transparent !important;
    border:none !important;
    box-shadow:none !important;
}
</style>
"""

    m.get_root().html.add_child(
        folium.Element(css)
    )

    for _, row in df.iterrows():

        try:

            if pd.isna(row["lat"]):
                continue

            popup = f"""
<b>{row['type']}</b><br><br>

💰 Prix : {row['prix']} €<br>

📍 CP : {row['cp']}<br><br>

<a href="{row['url']}" target="_blank">
Voir annonce
</a>
"""

            html = """
<div style="
background:red;
width:18px;
height:18px;
border-radius:50%;
border:2px solid white;
"></div>
"""

            marker = folium.Marker(

                location=[
                    row["lat"],
                    row["lon"]
                ],

                popup=folium.Popup(
                    popup,
                    max_width=350
                ),

                icon=DivIcon(
                    html=html,
                    icon_size=(18, 18),
                    icon_anchor=(9, 9)
                )
            )

            marker.add_to(m)

        except Exception as e:

            print(e)

    m.save(OUTPUT_MAP)

    print("✅ CARTE SAUVEGARDEE")


# =========================================================
# SCRAPE
# =========================================================

async def scrape():

    rows = []

    seen = set()

    print("")
    print("================================================")
    print("🌐 OUVERTURE")
    print("================================================")

    async with async_playwright() as p:

        browser = await p.chromium.launch(

            headless=False,

            slow_mo=50,

            args=[

                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage"

            ]
        )

        context = await browser.new_context(

            viewport={
                "width": 1366,
                "height": 900
            },

            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",

            locale="fr-FR",

            timezone_id="Europe/Paris"
        )

        page = await context.new_page()

        # =====================================================
        # ANTI DETECTION
        # =====================================================

        await page.add_init_script("""

Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined
})

""")

        # =====================================================
        # OPEN
        # =====================================================

        await page.goto(
            BASE_URL,
            wait_until="networkidle",
            timeout=120000
        )

        await page.wait_for_timeout(8000)

        # =====================================================
        # COOKIE
        # =====================================================

        print("")
        print("================================================")
        print("🍪 COOKIES")
        print("================================================")

        try:

            await page.locator(
                "button:has-text('Accepter')"
            ).click(timeout=5000)

            print("🍪 COOKIE OK")

        except:

            print("⚠️ PAS DE POPUP COOKIE")

        # =====================================================
        # SCROLL
        # =====================================================

        print("")
        print("================================================")
        print("🖱️ SCROLL")
        print("================================================")

        for i in range(15):

            print(f"🖱️ {i+1}/15")

            await page.mouse.wheel(0, 2500)

            await page.wait_for_timeout(2000)

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
        # EXTRACTION
        # =====================================================

        print("")
        print("================================================")
        print("🔗 EXTRACTION")
        print("================================================")

        cards = page.locator(
            "a[href*='/vente/']"
        )

        count = await cards.count()

        print(f"🧩 TOTAL ANNONCES = {count}")

        # =====================================================
        # LOOP
        # =====================================================

        for i in range(count):

            try:

                print("-" * 60)

                el = cards.nth(i)

                url = await el.get_attribute("href")

                txt = clean(
                    await el.inner_text()
                )

                print(txt[:500])

                if not url:
                    continue

                if "/vente/" not in url:
                    continue

                if not url.startswith("http"):

                    url = (
                        "https://avoventes.fr"
                        + url
                    )

                if url in seen:
                    continue

                if len(txt) < 20:
                    continue

                seen.add(url)

                row = {

                    "url": url,

                    "prix": extract_price(txt),

                    "cp": extract_cp(txt),

                    "type": detect_type(txt),

                    "txt": txt

                }

                rows.append(row)

                print("")
                print("✅ ANNONCE")
                print(f"TYPE = {row['type']}")
                print(f"PRIX = {row['prix']}")
                print(f"CP = {row['cp']}")
                print(f"URL = {row['url']}")

            except Exception as e:

                print("")
                print("❌ ERREUR")
                print(e)

        await browser.close()

    df = pd.DataFrame(rows)

    if len(df) > 0:

        df = df.drop_duplicates(
            subset=["url"]
        )

    print("")
    print("================================================")
    print("📦 RESULTAT")
    print("================================================")

    print(df.head())

    print(f"📦 TOTAL = {len(df)}")

    return df


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

        # =====================================================
        # SAVE CSV
        # =====================================================

        df.to_csv(
            OUTPUT_CSV,
            sep=";",
            index=False,
            encoding="utf-8-sig"
        )

        print("✅ CSV")

        # =====================================================
        # GEO
        # =====================================================

        df = geolocate(df)

        # =====================================================
        # MAP
        # =====================================================

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
