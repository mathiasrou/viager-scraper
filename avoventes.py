# =========================================================
# AVOVENTES.FR
# VERSION DEBUG COMPLETE
# GITHUB / PLAYWRIGHT / API DETECTION
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


def extract_surface(txt):

    try:

        matches = re.findall(

            r"(\d+(?:[.,]\d+)?)\s?(?:m²|m2|M²|M2)",

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
        pass

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
        f"📍 {df['lat'].notna().sum()} annonces géolocalisées"
    )

    return df


# =========================================================
# SCRAPE
# =========================================================

async def scrape():

    rows = []

    seen = set()

    api_jsons = []

    print("")
    print("================================================")
    print("🚀 DEBUT SCRAPE AVOVENTES")
    print("================================================")

    async with async_playwright() as p:

        print("")
        print("================================================")
        print("🌐 OUVERTURE NAVIGATEUR")
        print("================================================")

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

            locale="fr-FR",

            viewport={
                "width": 1600,
                "height": 900
            }
        )

        page = await context.new_page()

        # =========================================
        # ANTI WEBDRIVER
        # =========================================

        await page.add_init_script("""

Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined
})

""")

        # =========================================
        # INTERCEPTION API
        # =========================================

        async def handle_response(response):

            try:

                url = response.url

                ctype = response.headers.get(
                    "content-type",
                    ""
                )

                if "json" in ctype.lower():

                    print("")
                    print("📡 JSON DETECTE")
                    print(url)

                    try:

                        data = await response.json()

                        api_jsons.append({

                            "url": url,

                            "data": data

                        })

                        print("✅ JSON CAPTURE")

                    except Exception as e:

                        print("❌ ERREUR JSON")
                        print(e)

            except:
                pass

        page.on(
            "response",
            handle_response
        )

        # =========================================
        # OUVERTURE PAGE
        # =========================================

        print("")
        print("================================================")
        print("🌐 OUVERTURE URL")
        print("================================================")
        print(BASE_URL)

        await page.goto(
            BASE_URL,
            timeout=120000
        )

        print("")
        print("================================================")
        print("⏳ ATTENTE CHARGEMENT")
        print("================================================")

        await page.wait_for_load_state(
            "networkidle"
        )

        await page.wait_for_timeout(15000)

        # =========================================
        # SCROLL
        # =========================================

        print("")
        print("================================================")
        print("🖱️ SCROLL")
        print("================================================")

        for i in range(15):

            print(f"🖱️ SCROLL {i+1}/15")

            await page.evaluate("""

window.scrollBy(
    0,
    window.innerHeight
)

""")

            await page.wait_for_timeout(2500)

        # =========================================
        # SAVE HTML
        # =========================================

        print("")
        print("================================================")
        print("💾 SAUVEGARDE HTML")
        print("================================================")

        html = await page.content()

        with open(
            "debug_avoventes.html",
            "w",
            encoding="utf-8"
        ) as f:

            f.write(html)

        print("✅ HTML SAUVEGARDE")

        # =========================================
        # SAVE JSON
        # =========================================

        print("")
        print("================================================")
        print("📡 JSONS CAPTURES")
        print("================================================")

        print(f"📡 TOTAL JSON = {len(api_jsons)}")

        with open(
            "debug_api.json",
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                api_jsons,
                f,
                ensure_ascii=False,
                indent=2
            )

        print("✅ JSON DEBUG SAUVEGARDE")

        # =========================================
        # PARSE JSON
        # =========================================

        print("")
        print("================================================")
        print("🔎 ANALYSE JSON")
        print("================================================")

        for block in api_jsons:

            try:

                txt = json.dumps(
                    block,
                    ensure_ascii=False
                )

                txt_low = txt.lower()

                if not any(

                    x in txt_low for x in [

                        "appartement",
                        "studio",
                        "maison",
                        "villa",
                        "terrain",
                        "immeuble"

                    ]
                ):
                    continue

                urls = re.findall(

                    r'https://[^"]+',

                    txt
                )

                if len(urls) == 0:
                    continue

                for u in urls:

                    if u in seen:
                        continue

                    if "avoventes.fr" not in u:
                        continue

                    seen.add(u)

                    row = {

                        "url": u,

                        "txt": txt[:3000],

                        "prix": extract_price(txt),

                        "surface": extract_surface(txt),

                        "cp": extract_cp(txt),

                        "pieces": extract_rooms(txt),

                        "type": detect_type(txt)

                    }

                    rows.append(row)

                    print("")
                    print("✅ ANNONCE")
                    print(f"TYPE : {row['type']}")
                    print(f"PRIX : {row['prix']}")
                    print(f"SURFACE : {row['surface']}")
                    print(f"CP : {row['cp']}")
                    print(f"URL : {row['url']}")

            except Exception as e:

                print("")
                print("❌ ERREUR PARSE JSON")
                print(e)

        await browser.close()

    df = pd.DataFrame(rows)

    if len(df) > 0:

        df = df.drop_duplicates(
            subset=["url"]
        )

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

📐 Surface : {row['surface']} m²<br>

🚪 Pièces : {row['pieces']}<br>

📍 CP : {row['cp']}<br><br>

<a href="{row['url']}" target="_blank">
Voir annonce
</a>
"""

            html = """
<div style="
background:red;
width:22px;
height:22px;
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
                    icon_size=(22, 22),
                    icon_anchor=(11, 11)
                )
            )

            marker.add_to(m)

        except Exception as e:

            print("❌ MARKER")
            print(e)

    m.save(OUTPUT_MAP)

    print("")
    print("================================================")
    print("🗺️ CARTE")
    print("================================================")
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

        print("")
        print("================================================")
        print("💾 CSV")
        print("================================================")

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

            f"✅ AVOVENTES\n"
            f"{len(df)} annonces"
        )

        print("")
        print("================================================")
        print("✅ FIN")
        print("================================================")

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
