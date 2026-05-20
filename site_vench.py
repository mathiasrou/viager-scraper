
# =========================================================
# VENCH SCRAPER FULL VERSION
# =========================================================
#
# FEATURES
# --------
#
# ✅ pagination
# ✅ scraping detail pages
# ✅ historique CSV
# ✅ telegram alert
# ✅ carte folium
# ✅ couleurs popup
# ✅ debug ultra verbeux
# ✅ screenshots
# ✅ html dumps
# ✅ txt dumps
# ✅ retry
# ✅ geolocalisation CP
# ✅ anti doublons
# ✅ extraction robuste
# ✅ surface
# ✅ tribunal
# ✅ date audience
# ✅ mise à prix
#
# =========================================================

import asyncio
import os
import re
import traceback
import json
import time

import pandas as pd
import requests
import folium

from bs4 import BeautifulSoup

from playwright.async_api import async_playwright

# =========================================================
# CONFIG
# =========================================================

BASE_URL = "https://www.vench.fr/prochaines-ventes-aux-encheres.html"

MAX_PAGES = 5

HEADLESS = True

DEBUG_DIR = "debug_vench"

OUTPUT_CSV = "vench.csv"

HISTO_CSV = "historique_vench.csv"

MAP_FILE = "carte_vench.html"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

os.makedirs(
    DEBUG_DIR,
    exist_ok=True
)

# =========================================================
# LOAD GEO
# =========================================================

geo = pd.read_csv(
    "base-officielle-codes-postaux.csv",
    dtype=str
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

geo["cp"] = geo["cp"].astype(str)

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
# DEBUG
# =========================================================

def log(title, value=""):

    print("")
    print("=" * 80)
    print(title)

    if value != "":
        print(value)

    print("=" * 80)

# =========================================================
# SAVE
# =========================================================

def save_txt(path, txt):

    with open(
        path,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(txt)

# =========================================================
# EXTRACTIONS
# =========================================================

def extract_price(txt):

    txt = clean(txt)

    patterns = [

        r"MISE\s*À\s*PRIX\s*[:\-]?\s*([\d\s\.,]+)\s*€",

        r"mise\s*à\s*prix\s*[:\-]?\s*([\d\s\.,]+)\s*€",

        r"([\d\s]+(?:[\.,]\d+)?)\s*€"

    ]

    found = []

    for p in patterns:

        matches = re.findall(
            p,
            txt,
            re.I
        )

        for m in matches:

            try:

                v = str(m)

                v = v.replace(" ", "")
                v = v.replace(".00", "")
                v = v.replace(",00", "")

                v = re.sub(r"[^\d]", "", v)

                if not v:
                    continue

                v = int(v)

                if 500 <= v <= 100000000:

                    found.append(v)

            except:
                pass

    if len(found) == 0:
        return None

    return min(found)


def extract_cp(txt):

    m = re.search(
        r"\b(\d{5})\b",
        txt
    )

    if m:
        return m.group(1)

    return None


def extract_surface(txt):

    m = re.search(
        r"(\d+(?:[\.,]\d+)?)\s?m²",
        txt,
        re.I
    )

    if m:
        return m.group(1)

    return None


def extract_date(txt):

    m = re.search(
        r"(\d{2}/\d{2}/\d{4})",
        txt
    )

    if m:
        return m.group(1)

    return None


def extract_tribunal(txt):

    m = re.search(
        r"TRIBUNAL[^A-Z]*([A-Z\-\s]+)",
        txt
    )

    if m:

        return clean(
            m.group(1)
        )

    return None


def detect_type(txt):

    txt = txt.lower()

    mapping = {

        "maison": "Maison",

        "appartement": "Appartement",

        "terrain": "Terrain",

        "parcelle": "Terrain",

        "immeuble": "Immeuble",

        "garage": "Garage",

        "local": "Local"

    }

    for k, v in mapping.items():

        if k in txt:

            return v

    return "Autre"

# =========================================================
# TELEGRAM
# =========================================================

def telegram(msg):

    try:

        if not TELEGRAM_TOKEN:
            return

        if not TELEGRAM_CHAT_ID:
            return

        url = (
            f"https://api.telegram.org/bot"
            f"{TELEGRAM_TOKEN}/sendMessage"
        )

        requests.post(

            url,

            data={

                "chat_id": TELEGRAM_CHAT_ID,

                "text": msg,

                "parse_mode": "HTML"

            },

            timeout=30
        )

        print("📨 TELEGRAM OK")

    except Exception as e:

        print("❌ TELEGRAM ERROR")
        print(e)

# =========================================================
# COLOR
# =========================================================

def color_price(price):

    if price is None:
        return "gray"

    if price < 10000:
        return "green"

    if price < 50000:
        return "orange"

    return "red"

# =========================================================
# PARSE DETAIL
# =========================================================

def parse_detail(html, url):

    soup = BeautifulSoup(
        html,
        "html.parser"
    )

    title = ""

    if soup.title:
        title = clean(
            soup.title.get_text()
        )

    h1 = ""

    h1_tag = soup.find("h1")

    if h1_tag:
        h1 = clean(
            h1_tag.get_text()
        )

    body = clean(
        soup.get_text(" ")
    )

    full = "\n".join([
        title,
        h1,
        body
    ])

    cp = extract_cp(full)

    price = extract_price(full)

    surface = extract_surface(full)

    date_vente = extract_date(full)

    tribunal = extract_tribunal(full)

    property_type = detect_type(full)

    row = {

        "url": url,

        "titre": h1,

        "cp": cp,

        "prix": price,

        "surface": surface,

        "date_vente": date_vente,

        "tribunal": tribunal,

        "type": property_type,

        "txt": full
    }

    log(
        "✅ EXTRACTION",
        json.dumps(
            row,
            indent=2,
            ensure_ascii=False
        )
    )

    return row

# =========================================================
# GEO
# =========================================================

def geocode_cp(df):

    df["cp"] = df["cp"].astype(str)

    out = df.merge(
        geo,
        on="cp",
        how="left"
    )

    return out

# =========================================================
# MAP
# =========================================================

def build_map(df):

    if len(df) == 0:
        return

    valid = df.dropna(
        subset=["lat", "lon"]
    )

    if len(valid) == 0:
        return

    m = folium.Map(

        location=[
            valid["lat"].astype(float).mean(),
            valid["lon"].astype(float).mean()
        ],

        zoom_start=6
    )

    for _, row in valid.iterrows():

        try:

            color = color_price(
                row["prix"]
            )

            popup = f"""
            <b>{row['type']}</b><br><br>

            💰 Prix :
            {row['prix']} €<br>

            📮 CP :
            {row['cp']}<br>

            🏛 Tribunal :
            {row['tribunal']}<br>

            📅 Audience :
            {row['date_vente']}<br>

            📐 Surface :
            {row['surface']}<br><br>

            <a href="{row['url']}" target="_blank">
            Ouvrir annonce
            </a>
            """

            folium.CircleMarker(

                location=[
                    float(row["lat"]),
                    float(row["lon"])
                ],

                radius=8,

                color=color,

                fill=True,

                fill_opacity=0.8,

                popup=folium.Popup(
                    popup,
                    max_width=350
                )

            ).add_to(m)

        except:
            pass

    m.save(MAP_FILE)

    log(
        "🗺 MAP SAVED",
        MAP_FILE
    )

# =========================================================
# HISTORIQUE
# =========================================================

def load_histo():

    if not os.path.exists(
        HISTO_CSV
    ):

        return set()

    try:

        histo = pd.read_csv(
            HISTO_CSV,
            sep=";"
        )

        return set(
            histo["id_unique"]
        )

    except:
        return set()

# =========================================================
# MAIN SCRAPER
# =========================================================

async def scrape():

    rows = []

    known = load_histo()

    log(
        "📚 HISTORIQUE",
        len(known)
    )

    async with async_playwright() as p:

        browser = await p.chromium.launch(

            headless=HEADLESS,

            args=[

                "--no-sandbox",

                "--disable-dev-shm-usage",

                "--disable-blink-features=AutomationControlled"

            ]
        )

        context = await browser.new_context(

            viewport={
                "width": 1920,
                "height": 1080
            },

            user_agent=(
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/122.0 Safari/537.36"
            )
        )

        page = await context.new_page()

        for page_num in range(
            1,
            MAX_PAGES + 1
        ):

            try:

                if page_num == 1:

                    url = BASE_URL

                else:

                    url = (
                        BASE_URL
                        + f"?p={page_num}"
                    )

                log(
                    f"📄 PAGE {page_num}",
                    url
                )

                await page.goto(

                    url,

                    wait_until="domcontentloaded",

                    timeout=120000
                )

                await page.wait_for_timeout(
                    5000
                )

                await page.screenshot(

                    path=(
                        f"{DEBUG_DIR}/"
                        f"page_{page_num}.png"
                    ),

                    full_page=True
                )

                html = await page.content()

                save_txt(

                    f"{DEBUG_DIR}/"
                    f"page_{page_num}.html",

                    html
                )

                links = await page.eval_on_selector_all(

                    "a",

                    """
                    els => els
                        .map(e => e.href)
                        .filter(h =>
                            h.includes('/vente-')
                        )
                    """
                )

                links = list(set(links))

                log(
                    "🏠 NB ANNONCES",
                    len(links)
                )

                # =========================================
                # DETAILS
                # =========================================

                for i, detail_url in enumerate(links):

                    try:

                        log(
                            f"🏠 DETAIL "
                            f"{i+1}/{len(links)}",
                            detail_url
                        )

                        detail = await context.new_page()

                        await detail.goto(

                            detail_url,

                            wait_until=(
                                "domcontentloaded"
                            ),

                            timeout=120000
                        )

                        await detail.wait_for_timeout(
                            3000
                        )

                        detail_html = await detail.content()

                        save_txt(

                            f"{DEBUG_DIR}/"
                            f"detail_{page_num}_{i}.html",

                            detail_html
                        )

                        await detail.screenshot(

                            path=(
                                f"{DEBUG_DIR}/"
                                f"detail_{page_num}_{i}.png"
                            ),

                            full_page=True
                        )

                        row = parse_detail(
                            detail_html,
                            detail_url
                        )

                        # =================================
                        # UNIQUE ID
                        # =================================

                        row["id_unique"] = (
                            f"{row['url']}"
                        )

                        # =================================
                        # NEW ?
                        # =================================

                        if row["id_unique"] not in known:

                            log(
                                "🆕 NOUVELLE ANNONCE"
                            )

                            msg = f"""
🏠 <b>{row['type']}</b>

💰 {row['prix']} €

📮 {row['cp']}

📅 {row['date_vente']}

🏛 {row['tribunal']}

🔗 {row['url']}
"""

                            telegram(msg)

                        rows.append(row)

                        await detail.close()

                    except Exception as e:

                        log(
                            "❌ DETAIL ERROR",
                            str(e)
                        )

                        traceback.print_exc()

                # =========================================
                # SAVE LIVE
                # =========================================

                live = pd.DataFrame(rows)

                live.to_csv(

                    OUTPUT_CSV,

                    sep=";",

                    index=False,

                    encoding="utf-8-sig"
                )

                log(
                    "💾 LIVE CSV SAVED",
                    len(live)
                )

            except Exception as e:

                log(
                    "❌ PAGE ERROR",
                    str(e)
                )

                traceback.print_exc()

        await browser.close()

    return pd.DataFrame(rows)

# =========================================================
# MAIN
# =========================================================

async def main():

    log("🚀 START")

    df = await scrape()

    # =====================================================
    # GEO
    # =====================================================

    log("🌍 GEO")

    df = geocode_cp(df)

    # =====================================================
    # SAVE FINAL
    # =====================================================

    df.to_csv(

        OUTPUT_CSV,

        sep=";",

        index=False,

        encoding="utf-8-sig"
    )

    # =====================================================
    # HISTORIQUE
    # =====================================================

    histo = df.copy()

    histo.to_csv(

        HISTO_CSV,

        sep=";",

        index=False,

        encoding="utf-8-sig"
    )

    log(
        "📚 HISTO SAVED",
        len(histo)
    )

    # =====================================================
    # MAP
    # =====================================================

    build_map(df)

    # =====================================================
    # STATS
    # =====================================================

    log(
        "📊 FINAL DF",
        df.shape
    )

    print(df.head())

    log("🏁 FIN")


asyncio.run(main())
