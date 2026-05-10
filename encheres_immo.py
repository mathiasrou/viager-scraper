# -*- coding: utf-8 -*-

import asyncio
import pandas as pd
import re
import folium
import os
import requests
import traceback

from datetime import datetime

from playwright.async_api import async_playwright
from folium.features import DivIcon


# =========================================================
# CONFIG
# =========================================================

URL = "https://encheres-immo.com/annonces"

HISTORY_FILE = "historique_immo.csv"

CP_FILE = "base-officielle-codes-postaux.csv"

HEADLESS = True

MAX_PAGES = 100


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram(message):

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:

        print("❌ TELEGRAM NON CONFIGURE")

        return

    try:

        url = (
            f"https://api.telegram.org/"
            f"bot{token}/sendMessage"
        )

        requests.post(

            url,

            data={

                "chat_id": chat_id,

                "text": str(message)[:4000]

            },

            timeout=30

        )

    except Exception as e:

        print("❌ ERREUR TELEGRAM")

        print(e)


def send_file(path):

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        return

    try:

        url = (
            f"https://api.telegram.org/"
            f"bot{token}/sendDocument"
        )

        with open(path, "rb") as f:

            requests.post(

                url,

                data={

                    "chat_id": chat_id

                },

                files={

                    "document": f

                },

                timeout=60

            )

    except Exception as e:

        print("❌ SEND FILE")

        print(e)


# =========================================================
# CLEAN
# =========================================================

def clean(txt):

    if pd.isna(txt):
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

def extract_type(txt):

    t = str(txt).lower()

    appartement_keywords = [

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
        "t6",

        "f1",
        "f2",
        "f3",
        "f4",
        "f5",

        "balcon",
        "étage",
        "ascenseur",
        "résidence"

    ]

    for k in appartement_keywords:

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

        values = []

        for m in matches:

            digits = re.sub(
                r"[^\d]",
                "",
                m
            )

            if digits == "":
                continue

            value = int(digits)

            if value < 1000:
                continue

            if value > 100000000:
                continue

            values.append(value)

        if len(values) == 0:
            return None

        return min(values)

    except:
        return None


def extract_surface(txt):

    try:

        m = re.search(
            r"(\d+)\s?m²",
            txt,
            re.I
        )

        if not m:
            return None

        return int(m.group(1))

    except:
        return None


def extract_cp(txt):

    try:

        matches = re.findall(
            r"\b(\d{5})\b",
            txt
        )

        if len(matches) == 0:
            return None

        return matches[0]

    except:
        return None


def extract_rooms(txt):

    try:

        m = re.search(
            r"(\d+)\s*pi[eè]ces",
            txt,
            re.I
        )

        if not m:
            return None

        return int(m.group(1))

    except:
        return None


def extract_bedrooms(txt):

    try:

        m = re.search(
            r"(\d+)\s*chambres?",
            txt,
            re.I
        )

        if not m:
            return None

        return int(m.group(1))

    except:
        return None


def extract_date(txt):

    try:

        m = re.search(
            r"(Débute|Termine)\s+le\s+(\d{2}/\d{2}/\d{4})",
            txt,
            re.I
        )

        if not m:
            return None

        return m.group(2)

    except:
        return None


def extract_status(txt):

    txt_low = txt.lower()

    if "vente terminée" in txt_low:
        return "terminee"

    if "termine le" in txt_low:
        return "en_cours"

    if "débute le" in txt_low:
        return "future"

    return "inconnu"


# =========================================================
# DATES
# =========================================================

def compute_days_remaining(date_str):

    try:

        if date_str is None:
            return ""

        d = datetime.strptime(
            date_str,
            "%d/%m/%Y"
        )

        delta = (d - datetime.now()).days

        return f"{delta}j"

    except:
        return ""


def short_date(date_str):

    try:

        if date_str is None:
            return ""

        d = datetime.strptime(
            date_str,
            "%d/%m/%Y"
        )

        return f"{d.day}/{d.month}"

    except:
        return ""


# =========================================================
# PRIX ICON
# =========================================================

def price_symbol(price):

    try:

        if price is None:
            return None

        if price < 400000:
            return None

        n = int(price / 100000)

        return f"{n}€"

    except:
        return None


# =========================================================
# COLOR
# =========================================================

def marker_color(price):

    try:

        if price is None:
            return "gray"

        if price < 100000:
            return "red"

        if price < 200000:
            return "purple"

        if price < 300000:
            return "green"

        return "gray"

    except:
        return "gray"


# =========================================================
# SCRAPE
# =========================================================

async def scrape():

    rows = []

    seen = set()

    # =====================================================
    # HISTORIQUE
    # =====================================================

    print("")
    print("=" * 60)
    print("📦 VERIFICATION HISTORIQUE")
    print("=" * 60)

    if os.path.exists(HISTORY_FILE):

        print(f"✅ FICHIER TROUVE : {HISTORY_FILE}")

        try:

            old = pd.read_csv(

                HISTORY_FILE,

                sep=";",

                encoding="utf-8-sig",

                low_memory=False

            )

            print("✅ CSV HISTORIQUE LU")

            print(f"📦 LIGNES = {len(old)}")

            print(f"📦 COLONNES = {list(old.columns)}")

            if "url" not in old.columns:

                print("❌ COLONNE URL ABSENTE")

                old_urls = set()

            else:

                old_urls = set(

                    old["url"]
                    .astype(str)
                    .tolist()

                )

                print(f"✅ URLS = {len(old_urls)}")

        except Exception as e:

            print("❌ ERREUR LECTURE HISTORIQUE")

            print(e)

            traceback.print_exc()

            old_urls = set()

    else:

        print("🆕 AUCUN HISTORIQUE")

        old_urls = set()

    # =====================================================
    # PLAYWRIGHT
    # =====================================================

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=HEADLESS
        )

        page = await browser.new_page()

        await page.goto(
            URL,
            timeout=60000
        )

        await page.wait_for_timeout(5000)

        page_num = 1

        while True:

            print("")
            print("=" * 60)
            print(f"📄 PAGE {page_num}")
            print("=" * 60)

            await page.wait_for_timeout(3000)

            articles = await page.query_selector_all(
                "article"
            )

            print(f"🧩 ARTICLES = {len(articles)}")

            if len(articles) == 0:
                break

            for article in articles:

                try:

                    txt = await article.inner_text()

                    txt = clean(txt)

                    if len(txt) < 20:
                        continue

                    annonce_url = None

                    links = await article.query_selector_all("a")

                    for link in links:

                        href = await link.get_attribute("href")

                        if not href:
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

                    # =========================================
                    # DOUBLONS
                    # =========================================

                    if annonce_url in seen:
                        continue

                    seen.add(annonce_url)

                    prix = extract_price(txt)

                    surface = extract_surface(txt)

                    cp = extract_cp(txt)

                    type_bien = extract_type(txt)

                    rooms = extract_rooms(txt)

                    bedrooms = extract_bedrooms(txt)

                    date_vente = extract_date(txt)

                    status = extract_status(txt)

                    rows.append({

                        "url": annonce_url,

                        "txt": txt,

                        "prix": prix,

                        "surface": surface,

                        "cp": cp,

                        "type": type_bien,

                        "pieces": rooms,

                        "chambres": bedrooms,

                        "date_vente": date_vente,

                        "status": status,

                        "is_new": annonce_url not in old_urls

                    })

                    print(
                        f"✅ {type_bien} | "
                        f"{prix}€ | "
                        f"{status}"
                    )

                except Exception as e:

                    print("❌ ARTICLE")

                    print(e)

            # =============================================
            # SUIVANT
            # =============================================

            try:

                next_btn = page.locator(
                    "text=Suivant"
                )

                count_next = await next_btn.count()

                print(f"➡️ BOUTON SUIVANT = {count_next}")

                if count_next == 0:
                    break

                await next_btn.first.click()

                await page.wait_for_timeout(5000)

            except Exception as e:

                print("❌ ERREUR SUIVANT")

                print(e)

                break

            page_num += 1

            if page_num > MAX_PAGES:
                break

        await browser.close()

    df = pd.DataFrame(rows)

    print("")
    print("=" * 60)
    print("📊 DATAFRAME SCRAPE")
    print("=" * 60)

    print(df.head())

    print(f"📦 TOTAL = {len(df)}")

    if len(df) > 0:

        before = len(df)

        df = df.drop_duplicates(
            subset=["url"],
            keep="first"
        )

        after = len(df)

        print(f"🧹 DOUBLONS SUPPRIMES = {before - after}")

    return df


# =========================================================
# GEO
# =========================================================

def geolocate(df):

    print("")
    print("=" * 60)
    print("🌍 GEOLOCALISATION")
    print("=" * 60)

    geo = pd.read_csv(

        CP_FILE,

        low_memory=False

    )

    print(f"📦 GEO LIGNES = {len(geo)}")

    print(f"📦 GEO COLONNES = {list(geo.columns)}")

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

    df["cp"] = df["cp"].astype(str)

    df = df.merge(

        geo,

        on="cp",

        how="left"

    )

    print("✅ GEO OK")

    return df


# =========================================================
# MAP
# =========================================================

def create_map(df):

    print("")
    print("=" * 60)
    print("🗺️ CREATION CARTE")
    print("=" * 60)

    active_df = df[
        df["status"] != "terminee"
    ].copy()

    active_df = active_df.drop_duplicates(
        subset=["url"],
        keep="first"
    )

    print(f"📦 ACTIFS = {len(active_df)}")

    m = folium.Map(

        location=[46.5, 2.5],

        zoom_start=6

    )

    used_positions = {}

    for _, row in active_df.iterrows():

        try:

            if pd.isna(row["lat"]):
                continue

            lat = float(row["lat"])
            lon = float(row["lon"])

            pos_key = f"{lat}_{lon}"

            if pos_key not in used_positions:

                used_positions[pos_key] = 0

            else:

                used_positions[pos_key] += 1

            offset = used_positions[pos_key]

            lat += offset * 0.0007
            lon += offset * 0.0007

            color = marker_color(
                row["prix"]
            )

            center_symbol = "🏠"

            if row["type"] == "Appartement":
                center_symbol = "🏢"

            if row["type"] == "Terrain":
                center_symbol = "🌳"

            if row["type"] == "Immeuble":
                center_symbol = "🏬"

            if row["type"] == "Villa":
                center_symbol = "🏡"

            symb = price_symbol(
                row["prix"]
            )

            if symb is not None:
                center_symbol = symb

            bottom_text = ""

            if row["status"] == "en_cours":

                bottom_text = compute_days_remaining(
                    row["date_vente"]
                )

            if row["status"] == "future":

                bottom_text = short_date(
                    row["date_vente"]
                )

            popup = f"""
            <b>{row['type']}</b><br><br>

            💰 Prix :
            {row['prix']} €<br>

            📐 Surface :
            {row['surface']} m²<br>

            🚪 Pièces :
            {row['pieces']}<br>

            🛏️ Chambres :
            {row['chambres']}<br>

            📅 Vente :
            {row['date_vente']}<br>

            📍 CP :
            {row['cp']}<br><br>

            <a href="{row['url']}"
            target="_blank">
            Voir annonce
            </a>
            """

            html = f"""
            <div style="
                display:flex;
                flex-direction:column;
                align-items:center;
                justify-content:center;
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
                    {center_symbol}
                </div>

                <div style="
                    font-size:11px;
                    font-weight:bold;
                    color:black;
                    margin-top:2px;
                    background:white;
                    padding:1px 4px;
                    border-radius:6px;
                    border:1px solid #999;
                ">
                    {bottom_text}
                </div>

            </div>
            """

            folium.Marker(

                [lat, lon],

                popup=folium.Popup(
                    popup,
                    max_width=350
                ),

                icon=DivIcon(
                    html=html
                )

            ).add_to(m)

        except Exception as e:

            print("❌ MARKER")

            print(e)

    m.save("carte_encheres_immo.html")

    print("✅ CARTE SAUVEGARDEE")


# =========================================================
# MAIN
# =========================================================

async def main():

    try:

        print("")
        print("=" * 60)
        print("🚀 DEMARRAGE")
        print("=" * 60)

        df = await scrape()

        print("")
        print("=" * 60)
        print("📊 CHECK DATAFRAME")
        print("=" * 60)

        print(type(df))

        print(f"📦 TOTAL = {len(df)}")

        if len(df) == 0:

            send_telegram(
                "😴 AUCUNE ANNONCE"
            )

            return

        # =================================================
        # GEO
        # =================================================

        df = geolocate(df)

        # =================================================
        # SAVE HISTORIQUE
        # =================================================

        print("")
        print("=" * 60)
        print("💾 SAVE HISTORIQUE")
        print("=" * 60)

        try:

            if os.path.exists(HISTORY_FILE):

                try:

                    old_save = pd.read_csv(

                        HISTORY_FILE,

                        sep=";",

                        encoding="utf-8-sig",

                        low_memory=False

                    )

                    print(f"📦 ANCIEN = {len(old_save)}")

                    df = pd.concat(

                        [old_save, df],

                        ignore_index=True

                    )

                except Exception as e:

                    print("❌ ERREUR RELECTURE")

                    print(e)

            before = len(df)

            df = df.drop_duplicates(

                subset=["url"],

                keep="first"

            )

            after = len(df)

            print(f"🧹 DOUBLONS = {before - after}")

            print(f"📦 FINAL = {len(df)}")

            print(f"📦 COLONNES = {list(df.columns)}")

            if "url" not in df.columns:

                raise Exception(
                    "COLONNE URL ABSENTE"
                )

            df.to_csv(

                HISTORY_FILE,

                sep=";",

                index=False,

                encoding="utf-8-sig"

            )

            print("✅ HISTORIQUE SAUVEGARDE")

        except Exception as e:

            print("❌ SAVE HISTORIQUE")

            print(e)

            traceback.print_exc()

        # =================================================
        # NEW
        # =================================================

        new_df = df[
            df["is_new"] == True
        ].copy()

        print(f"🔥 NOUVELLES = {len(new_df)}")

        if len(new_df) > 0:

            send_telegram(
                f"🔥 {len(new_df)} nouvelles annonces Encheres Immo"
            )

            for _, row in new_df.iterrows():

                msg = ""

                msg += f"🏠 {row['type']}\n\n"

                msg += f"💰 Prix : {row['prix']} €\n"

                msg += f"📐 Surface : {row['surface']} m²\n"

                msg += f"🚪 Pièces : {row['pieces']}\n"

                msg += f"🛏️ Chambres : {row['chambres']}\n"

                msg += f"📅 Vente : {row['date_vente']}\n"

                msg += f"📊 Status : {row['status']}\n"

                msg += f"📍 CP : {row['cp']}\n\n"

                msg += f"🔗 {row['url']}"

                send_telegram(msg)

        else:

            send_telegram(
                "😴 Aucune nouvelle annonce Encheres Immo"
            )

        # =================================================
        # MAP
        # =================================================

        create_map(df)

        send_file(
            "carte_encheres_immo.html"
        )

        print("")
        print("=" * 60)
        print("✅ FIN")
        print("=" * 60)

    except Exception as e:

        print("")
        print("=" * 60)
        print("❌ ERREUR MAIN")
        print("=" * 60)

        print(e)

        traceback.print_exc()

        send_telegram(
            f"❌ ERREUR ENCHERES IMMO\n\n{str(e)}"
        )


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
