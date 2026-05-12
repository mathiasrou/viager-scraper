# =========================================================
# IMMO NOTAIRES ENCHERES
# VERSION COMPLETE CORRIGEE
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

BASE_URL = "https://immonotairesencheres.com/bien"

CSV_CP = "base-officielle-codes-postaux.csv"

HISTORY_FILE = "historique_immonotaires.csv"

OUTPUT_MAP = "carte_immonotaires.html"


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
            r"(\d+(?:[.,]\d+)?)\s?M2",
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

        page_num = 0

        while True:

            url = (
                f"{BASE_URL}?page={page_num}"
            )

            print("")
            print("=" * 60)
            print(f"📄 PAGE {page_num}")
            print("=" * 60)

            print(url)

            try:

                await page.goto(
                    url,
                    timeout=60000
                )

            except Exception as e:

                print("❌ PAGE")
                print(e)

                break

            await page.wait_for_timeout(5000)

            articles = await page.query_selector_all(
                "article.node-property"
            )

            print(
                f"🧩 ARTICLES = {len(articles)}"
            )

            if len(articles) == 0:
                break

            count_before = len(rows)

            for article in articles:

                try:

                    txt = clean(
                        await article.inner_text()
                    )

                    if len(txt) < 20:
                        continue

                    link = await article.query_selector(
                        "a.button"
                    )

                    if not link:
                        continue

                    href = await link.get_attribute(
                        "href"
                    )

                    if not href:
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
                        f"{row['surface']}m²"
                    )

                except Exception as e:

                    print("❌ ARTICLE")
                    print(e)

            if len(rows) == count_before:

                print("⛔ FIN PAGINATION")
                break

            page_num += 1

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

        print("🚀 SCRAPING")

        df = await scrape()

        print("")
        print(f"📦 TOTAL = {len(df)}")

        if len(df) == 0:

            print("❌ AUCUNE ANNONCE")

            return

        df.to_csv(

            HISTORY_FILE,

            sep=";",

            index=False,

            encoding="utf-8-sig"

        )

        print("💾 HISTORIQUE")

        df = geolocate(df)

        print("📍 GEO OK")

        create_map(df)

        send_file(OUTPUT_MAP)

        send_telegram(
            f"✅ IMMO NOTAIRES\n"
            f"{len(df)} annonces"
        )

        print("✅ FIN")

    except Exception as e:

        print("❌ MAIN")
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
