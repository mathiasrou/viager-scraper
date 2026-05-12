# =========================================================
# AVOVENTES SCRAPER
# VERSION DOM DIRECT
# =========================================================

import asyncio
import pandas as pd
import re
import folium
import os
import requests

from playwright.async_api import async_playwright


# =========================================================
# CONFIG
# =========================================================

BASE_URL = "https://avoventes.fr/recherche/toutes"

CSV_CP = "base-officielle-codes-postaux.csv"

OUTPUT_MAP = "carte_avoventes.html"

CSV_OUTPUT = "historique_avoventes.csv"


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram(msg):

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    requests.post(
        url,
        data={
            "chat_id": chat_id,
            "text": msg
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

    txt = re.sub(r"\s+", " ", txt)

    return txt.strip()


# =========================================================
# EXTRACTIONS
# =========================================================

def extract_price(txt):

    try:

        vals = re.findall(
            r"(\d[\d\s]+)\s?€",
            txt
        )

        out = []

        for v in vals:

            v = re.sub(r"\s+", "", v)

            if v.isdigit():

                v = int(v)

                if 1000 <= v <= 100000000:
                    out.append(v)

        if len(out) == 0:
            return None

        return min(out)

    except:
        return None


def extract_cp(txt):

    try:

        m = re.findall(r"\b(\d{5})\b", txt)

        if len(m):
            return m[0]

    except:
        pass

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
        f"📍 GEO OK = {df['lat'].notna().sum()}"
    )

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

            popup = f"""
<b>{row['type']}</b><br><br>

💰 {row['prix']} €<br>

📍 {row['cp']}<br><br>

<a href="{row['url']}" target="_blank">
Annonce
</a>
"""

            folium.Marker(
                [row["lat"], row["lon"]],
                popup=popup
            ).add_to(m)

        except:
            pass

    m.save(OUTPUT_MAP)

    print("✅ CARTE SAUVEGARDEE")


# =========================================================
# SCRAPE
# =========================================================

async def scrape():

    rows = []

    seen = set()

    async with async_playwright() as p:

        browser = await p.chromium.launch(

            headless=True,

            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage"
            ]
        )

        page = await browser.new_page()

        print("")
        print("================================================")
        print("🌐 OUVERTURE")
        print("================================================")

        await page.goto(
            BASE_URL,
            timeout=120000
        )

        await page.wait_for_timeout(10000)

        # =====================================================
        # SCROLL
        # =====================================================

        print("")
        print("================================================")
        print("🖱️ SCROLL")
        print("================================================")

        for i in range(15):

            print(f"🖱️ {i+1}/15")

            await page.mouse.wheel(0, 4000)

            await page.wait_for_timeout(2000)

        # =====================================================
        # DEBUG
        # =====================================================

        await page.screenshot(
            path="debug_avoventes.png",
            full_page=True
        )

        html = await page.content()

        with open(
            "debug_avoventes.html",
            "w",
            encoding="utf-8"
        ) as f:

            f.write(html)

        # =====================================================
        # CARDS
        # =====================================================

        print("")
        print("================================================")
        print("🧩 CARDS")
        print("================================================")

        cards = page.locator(".card")

        count = await cards.count()

        print(f"🧩 TOTAL CARDS = {count}")

        # =====================================================
        # LOOP
        # =====================================================

        for i in range(count):

            try:

                card = cards.nth(i)

                txt = clean(
                    await card.inner_text()
                )

                print("")
                print("------------------------------------------------")
                print(f"CARD {i+1}")
                print("------------------------------------------------")

                print(txt[:500])

                links = await card.locator("a").evaluate_all(
                    """
                    els => els.map(e => e.href)
                    """
                )

                if len(links) == 0:

                    print("❌ PAS DE LIEN")

                    continue

                url = links[0]

                if url in seen:
                    continue

                seen.add(url)

                row = {

                    "url": url,

                    "txt": txt,

                    "prix": extract_price(txt),

                    "cp": extract_cp(txt),

                    "type": detect_type(txt)

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
                print("❌ ERREUR CARD")
                print(e)

        await browser.close()

    df = pd.DataFrame(rows)

    df = df.drop_duplicates(
        subset=["url"]
    )

    print("")
    print("================================================")
    print("📦 RESULTAT")
    print("================================================")

    print(df.head())

    print("")
    print(f"📦 TOTAL = {len(df)}")

    return df


# =========================================================
# MAIN
# =========================================================

async def main():

    try:

        df = await scrape()

        if len(df) == 0:

            print("❌ AUCUNE ANNONCE")

            send_telegram(
                "❌ AVOVENTES VIDE"
            )

            return

        df.to_csv(
            CSV_OUTPUT,
            sep=";",
            index=False,
            encoding="utf-8-sig"
        )

        print("✅ CSV")

        df = geolocate(df)

        create_map(df)

        send_telegram(
            f"✅ AVOVENTES : {len(df)} annonces"
        )

    except Exception as e:

        print("")
        print("================================================")
        print("❌ ERREUR")
        print("================================================")

        print(e)

        send_telegram(
            f"❌ ERREUR AVOVENTES\n{e}"
        )


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
