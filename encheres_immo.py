# =========================================================
# ENCHERES IMMO
# VERSION DEFINITIVE SANS BLEU
# =========================================================

import asyncio
import pandas as pd
import re
import folium
import traceback
import os
import requests

from datetime import datetime

from playwright.async_api import async_playwright

from folium.features import DivIcon


# =========================================================
# CONFIG
# =========================================================

BASE_URL = "https://encheres-immo.com/annonces"

CSV_CP = "base-officielle-codes-postaux.csv"

HISTORY_FILE = "historique_immo.csv"

OUTPUT_MAP = "carte_encheres_immo.html"


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

    appart = [

        "appartement",
        "studio",
        "duplex",
        "triplex",
        "loft",
        "t1",
        "t2",
        "t3",
        "t4",
        "t5",
        "f1",
        "f2",
        "f3",
        "f4",
        "f5"

    ]

    for k in appart:

        if k in t:
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

        m = re.search(
            r"(\d+)\s?m²",
            txt,
            re.I
        )

        if m:
            return int(m.group(1))

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
            r"(\d+)\s*pi[eè]ces",
            txt,
            re.I
        )

        if m:
            return int(m.group(1))

    except:
        pass

    return None


def extract_bedrooms(txt):

    try:

        m = re.search(
            r"(\d+)\s*chambres?",
            txt,
            re.I
        )

        if m:
            return int(m.group(1))

    except:
        pass

    return None


def extract_date(txt):

    try:

        m = re.search(
            r"(Débute|Termine)\s+le\s+(\d{2}/\d{2}/\d{4})",
            txt,
            re.I
        )

        if m:
            return m.group(2)

    except:
        pass

    return None


def extract_status(txt):

    t = txt.lower()

    if "vente terminée" in t:
        return "terminee"

    if "termine le" in t:
        return "en_cours"

    if "débute le" in t:
        return "future"

    return "inconnu"


# =========================================================
# MAP TOOLS
# =========================================================

def marker_color(price):

    if price is None:
        return "#777777"

    if price < 100000:
        return "#ff0000"

    if price < 200000:
        return "#8000ff"

    if price < 300000:
        return "#00aa00"

    return "#666666"


def marker_symbol(row):

    price = row.get("prix")

    t = row.get("type")

    if price is not None and price >= 400000:

        n = int(price / 100000)

        return f"{n}€"

    if t == "Appartement":
        return "🏢"

    if t == "Maison":
        return "🏠"

    if t == "Villa":
        return "🏡"

    if t == "Terrain":
        return "🌳"

    if t == "Immeuble":
        return "🏬"

    return "€"


def bottom_text(row):

    try:

        status = row.get("status")

        date_str = row.get("date_vente")

        if not date_str:
            return ""

        d = datetime.strptime(
            date_str,
            "%d/%m/%Y"
        )

        if status == "future":

            return f"{d.day}/{d.month}"

        if status == "en_cours":

            delta = (
                d - datetime.now()
            ).days

            return f"{delta}j"

    except:
        pass

    return ""


# =========================================================
# SCRAPE
# =========================================================

async def scrape():

    rows = []

    seen = set()

    if os.path.exists(HISTORY_FILE):

        try:

            old_df = pd.read_csv(
                HISTORY_FILE,
                sep=";"
            )

        except:

            old_df = pd.DataFrame()

    else:

        old_df = pd.DataFrame()

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=True
        )

        page = await browser.new_page()

        print("🌐 OUVERTURE")

        await page.goto(
            BASE_URL,
            timeout=60000
        )

        await page.wait_for_timeout(5000)

        page_num = 1

        while True:

            print("")
            print("=" * 60)
            print(f"📄 PAGE {page_num}")
            print("=" * 60)

            articles = await page.query_selector_all(
                "article"
            )

            print(f"🧩 ARTICLES = {len(articles)}")

            if len(articles) == 0:
                break

            for article in articles:

                try:

                    txt = clean(
                        await article.inner_text()
                    )

                    if len(txt) < 20:
                        continue

                    annonce_url = None

                    links = await article.query_selector_all(
                        "a"
                    )

                    for link in links:

                        href = await link.get_attribute(
                            "href"
                        )

                        if href is None:
                            continue

                        if href.startswith("/"):

                            annonce_url = (
                                "https://encheres-immo.com"
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

                        "type": detect_type(txt),

                        "pieces": extract_rooms(txt),

                        "chambres": extract_bedrooms(txt),

                        "date_vente": extract_date(txt),

                        "status": extract_status(txt)

                    }

                    rows.append(row)

                    print(
                        f"✅ {row['type']} | "
                        f"{row['prix']}€ | "
                        f"{row['status']}"
                    )

                except Exception as e:

                    print("❌ ARTICLE")
                    print(e)

            try:

                next_btn = page.locator(
                    "text=Suivant"
                )

                count = await next_btn.count()

                print(f"➡️ NEXT = {count}")

                if count == 0:
                    break

                await next_btn.first.click()

                await page.wait_for_timeout(5000)

            except Exception as e:

                print("❌ NEXT")
                print(e)

                break

            page_num += 1

        await browser.close()

    df = pd.DataFrame(rows)

    if len(old_df) > 0:

        df = pd.concat(
            [old_df, df],
            ignore_index=True
        )

    if len(df) > 0:

        df = df.drop_duplicates(
            subset=["url"],
            keep="first"
        )

    return df


# =========================================================
# GEO
# =========================================================

def geolocate(df):

    geo = pd.read_csv(
        CSV_CP,
        sep=";"
    )

    geo.columns = [
        c.lower()
        for c in geo.columns
    ]

    geo = geo[[
        "Code_postal",
        "Latitude",
        "Longitude"
    ]]

    geo.columns = [
        "cp",
        "lat",
        "lon"
    ]

    geo["cp"] = geo["cp"].astype(str)

    df["cp"] = df["cp"].astype(str)

    df = df.merge(
        geo,
        on="cp",
        how="left"
    )

    return df


# =========================================================
# CREATE MAP
# =========================================================

# =========================================================
# SOLUTION DEFINITIVE :
# LE BLEU VIENT DU CSS LEAFLET SUR DIVICON
# IL FAUT SUPPRIMER COMPLETEMENT LE STYLE PAR DEFAUT
# =========================================================

# REMPLACE UNIQUEMENT LA FONCTION create_map()

def create_map(df):
    active_df = df[df["status"] != "terminee"].copy()
    active_df = active_df.drop_duplicates(subset=["url"])

    m = folium.Map(location=[46.5, 2.5], zoom_start=6, tiles="CartoDB positron")

    # CSS anti-bleu
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
    m.get_root().html.add_child(folium.Element(css))

    for _, row in active_df.iterrows():
        try:
            if pd.isna(row["lat"]):
                continue

            prix = row["prix"]
            if prix is None:
                color = "#666666"
            elif prix < 100000:
                color = "#ff0000"
            elif prix < 200000:
                color = "#8000ff"
            elif prix < 300000:
                color = "#00aa00"
            else:
                color = "#666666"

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

            if prix is not None and prix >= 400000:
                symbol = f"{int(prix/100000)}€"

            bottom = ""
            try:
                if row["date_vente"]:
                    d = datetime.strptime(row["date_vente"], "%d/%m/%Y")
                    if row["status"] == "future":
                        bottom = f"{d.day}/{d.month}"
                    elif row["status"] == "en_cours":
                        delta = (d - datetime.now()).days
                        bottom = f"{delta}j"
            except:
                pass

            # POPUP - Sans aucune indentation au début des lignes
            popup = """
<b>{type}</b><br><br>
💰 Prix : {prix} €<br>
📐 Surface : {surface} m²<br>
🚪 Pièces : {pieces}<br>
🛏️ Chambres : {chambres}<br>
📅 Vente : {date_vente}<br>
📍 CP : {cp}<br><br>
<a href="{url}" target="_blank">Voir annonce</a>
""".format(
    type=row['type'],
    prix=row['prix'],
    surface=row['surface'],
    pieces=row['pieces'],
    chambres=row['chambres'],
    date_vente=row['date_vente'],
    cp=row['cp'],
    url=row['url']
)

            # HTML du marqueur - Même principe
            html = """
<div style="width:42px; display:flex; flex-direction:column; align-items:center; justify-content:center; background:transparent;">
    <div style="background:{color}; width:38px; height:38px; border-radius:50%; display:flex; align-items:center; justify-content:center; color:white; font-weight:bold; font-size:14px; border:2px solid white; box-shadow:0 0 4px rgba(0,0,0,0.4);">
        {symbol}
    </div>
    <div style="font-size:11px; font-weight:bold; color:black; background:white; padding:1px 4px; border-radius:6px; margin-top:2px; border:1px solid #999; white-space:nowrap;">
        {bottom}
    </div>
</div>
""".format(color=color, symbol=symbol, bottom=bottom)

            marker = folium.Marker(
                location=[row["lat"], row["lon"]],
                popup=folium.Popup(popup, max_width=350),
                icon=DivIcon(
                    html=html,
                    class_name="my-div-icon",
                    icon_size=(42, 42),
                    icon_anchor=(21, 21)
                )
            )
            marker.add_to(m)

        except Exception as e:
            print("❌ ERREUR MARKER")
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

        print(f"📦 TOTAL = {len(df)}")

        if len(df) == 0:
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
            f"✅ ENCHERES IMMO\n"
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
