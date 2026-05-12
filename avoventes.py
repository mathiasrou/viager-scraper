# =========================================================
# AVOVENTES.FR
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
# GEOLOCALISATION
# =========================================================

def geolocate(df):

    geo = pd.read_csv(
        CSV_CP
    )

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

    print(

        "📍 GEOLOCALISATION OK :",

        df["lat"].notna().sum(),

        "annonces"
    )

    return df


# =========================================================
# SCRAPE
# =========================================================

async def scrape():

    rows = []

    seen = set()

    async with async_playwright() as p:

        browser = await p.chromium.launch(

            headless=True
        )

        page = await browser.new_page()

        print("🌐 OUVERTURE")

        await page.goto(

            BASE_URL,

            timeout=90000
        )

        # attente JS
        await page.wait_for_timeout(8000)

        # scroll progressif
        for i in range(10):

            print(f"🖱️ SCROLL {i+1}/10")

            await page.mouse.wheel(0, 5000)

            await page.wait_for_timeout(1500)

        print("🔎 RECHERCHE DES BLOCS")

        selectors = [

            ".item",
            ".bien",
            ".card",
            ".card-body",
            ".annonce",
            ".result",
            ".property",
            ".vente",
            "article",
            ".grid-item"

        ]

        articles = []

        for sel in selectors:

            try:

                found = await page.query_selector_all(sel)

                print(f"{sel} -> {len(found)}")

                if len(found) > len(articles):

                    articles = found

            except:
                pass

        print("")
        print(f"🧩 TOTAL BLOCS = {len(articles)}")

        for article in articles:

            try:

                txt = clean(
                    await article.inner_text()
                )

                if len(txt) < 40:
                    continue

                txt_low = txt.lower()

                # filtre immobilier
                if not any(

                    x in txt_low for x in [

                        "appartement",
                        "studio",
                        "maison",
                        "villa",
                        "terrain",
                        "immeuble",
                        "atelier"

                    ]
                ):
                    continue

                links = await article.query_selector_all("a")

                annonce_url = None

                for link in links:

                    href = await link.get_attribute("href")

                    if not href:
                        continue

                    if (
                        "/vente/"
                        not in href
                        and "/annonce/"
                        not in href
                        and "/bien/"
                        not in href
                    ):
                        continue

                    if href.startswith("/"):

                        annonce_url = (
                            "https://avoventes.fr"
                            + href
                        )

                    else:

                        annonce_url = href

                    break

                if annonce_url is None:
                    continue

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
                    f"{row['surface']}m²"
                )

            except Exception as e:

                print("❌ ARTICLE")
                print(e)

        await browser.close()

    df = pd.DataFrame(rows)

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
.my-div-icon{
    background:transparent !important;
    border:none !important;
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

            prix = row["prix"]

            if prix is None:

                color = "#666666"

            elif prix < 100000:

                color = "#ff0000"

            elif prix < 300000:

                color = "#8000ff"

            elif prix < 700000:

                color = "#00aa00"

            else:

                color = "#222222"

            symbol = "€"

            if row["type"] == "Appartement":
                symbol = "🏢"

            elif row["type"] == "Maison":
                symbol = "🏠"

            elif row["type"] == "Villa":
                symbol = "🏡"

            elif row["type"] == "Terrain":
                symbol = "🌳"

            elif row["type"] == "Immeuble":
                symbol = "🏬"

            elif row["type"] == "Atelier":
                symbol = "🏭"

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

            html = f"""
<div style="
width:42px;
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
background:transparent;
">

<div style="
background:{color};
width:38px;
height:38px;
border-radius:50%;
display:flex;
align-items:center;
justify-content:center;
color:white;
font-weight:bold;
font-size:14px;
border:2px solid white;
box-shadow:0 0 4px rgba(0,0,0,0.4);
">
{symbol}
</div>

</div>
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

                    class_name="my-div-icon",

                    icon_size=(42, 42),

                    icon_anchor=(21, 21)
                )
            )

            marker.add_to(m)

        except Exception as e:

            print("❌ MARKER")
            print(e)

    m.save(OUTPUT_MAP)

    print("✅ CARTE SAUVEGARDEE")


# =========================================================
# MAIN
# =========================================================

async def main():

    try:

        print("🚀 SCRAPING AVOVENTES")

        df = await scrape()

        print("")
        print(f"📦 TOTAL = {len(df)}")

        if len(df) == 0:

            print("❌ AUCUNE ANNONCE")

            send_telegram(
                "❌ AVOVENTES : aucune annonce détectée"
            )

            return

        df.to_csv(

            HISTORY_FILE,

            sep=";",

            index=False,

            encoding="utf-8-sig"
        )

        print("💾 HISTORIQUE SAUVEGARDE")

        df = geolocate(df)

        print("📍 GEO OK")

        create_map(df)

        send_file(OUTPUT_MAP)

        send_telegram(

            f"✅ AVOVENTES\n"
            f"{len(df)} annonces"
        )

        print("✅ FIN")

    except Exception as e:

        print("❌ MAIN")
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
