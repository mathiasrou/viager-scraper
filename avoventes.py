# =========================================================
# AVOVENTES SCRAPER
# DEBUG MAXIMAL
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
# TYPE
# =========================================================

def detect_type(txt):

    t = txt.lower()

    if "appartement" in t:
        return "Appartement"

    if "studio" in t:
        return "Studio"

    if "maison" in t:
        return "Maison"

    if "villa" in t:
        return "Villa"

    if "terrain" in t:
        return "Terrain"

    if "immeuble" in t:
        return "Immeuble"

    if "garage" in t:
        return "Garage"

    return "Autre"


# =========================================================
# PRIX
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


# =========================================================
# CP
# =========================================================

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

    print("")
    print("================================================")
    print("📍 GEOLOCALISATION")
    print("================================================")

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

    print(f"📍 GEO OK = {df['lat'].notna().sum()}")

    return df


# =========================================================
# MAP
# =========================================================

def create_map(df):

    print("")
    print("================================================")
    print("🗺️ CREATION CARTE")
    print("================================================")

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

💰 Prix : {row['prix']} €<br>

📍 CP : {row['cp']}<br><br>

<a href="{row['url']}" target="_blank">
Voir annonce
</a>
"""

            marker = folium.Marker(

                location=[
                    row["lat"],
                    row["lon"]
                ],

                popup=popup

            )

            marker.add_to(m)

        except Exception as e:

            print("❌ ERREUR MARKER")
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
    print("🚀 DEBUT SCRAPE AVOVENTES")
    print("================================================")

    async with async_playwright() as p:

        # =================================================
        # BROWSER
        # =================================================

        print("")
        print("================================================")
        print("🌐 OUVERTURE NAVIGATEUR")
        print("================================================")

        browser = await p.chromium.launch(

            headless=False,

            args=[

                "--no-sandbox",
                "--disable-dev-shm-usage"

            ]
        )

        # =================================================
        # CONTEXT
        # =================================================

        print("")
        print("================================================")
        print("🧠 CREATION CONTEXT")
        print("================================================")

        context = await browser.new_context(

            viewport={
                "width": 1600,
                "height": 900
            },

            locale="fr-FR",

            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )

        # =================================================
        # PAGE
        # =================================================

        print("")
        print("================================================")
        print("📄 CREATION PAGE")
        print("================================================")

        page = await context.new_page()

        # =================================================
        # OPEN URL
        # =================================================

        print("")
        print("================================================")
        print("🌐 OUVERTURE URL")
        print("================================================")

        print(BASE_URL)

        await page.goto(
            BASE_URL,
            timeout=120000
        )

        # =================================================
        # WAIT
        # =================================================

        print("")
        print("================================================")
        print("⏳ ATTENTE CHARGEMENT")
        print("================================================")

        await page.wait_for_timeout(10000)

        # =================================================
        # COOKIES
        # =================================================

        print("")
        print("================================================")
        print("🍪 COOKIES")
        print("================================================")

        try:

            buttons = [

                "button:has-text('Accepter')",
                "button:has-text('Tout accepter')",
                "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"

            ]

            clicked = False

            for b in buttons:

                try:

                    await page.locator(b).click(timeout=3000)

                    print(f"✅ COOKIE CLICK : {b}")

                    clicked = True

                    break

                except:
                    pass

            if not clicked:
                print("⚠️ PAS DE POPUP COOKIE")

        except Exception as e:

            print("❌ ERREUR COOKIE")
            print(e)

        # =================================================
        # SCROLL
        # =================================================

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

        # =================================================
        # END PAGE
        # =================================================

        print("")
        print("================================================")
        print("🔚 FIN PAGE")
        print("================================================")

        await page.keyboard.press("End")

        await page.wait_for_timeout(5000)

        # =================================================
        # SCREENSHOT
        # =================================================

        print("")
        print("================================================")
        print("🖼️ SCREENSHOT")
        print("================================================")

        await page.screenshot(

            path="debug_avoventes.png",
            full_page=True

        )

        print("✅ SCREENSHOT SAUVEGARDE")

        # =================================================
        # HTML
        # =================================================

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

        # =================================================
        # LINKS
        # =================================================

        print("")
        print("================================================")
        print("🔗 EXTRACTION LINKS")
        print("================================================")

        links = await page.locator("a").evaluate_all("""

els => els.map(e => ({

    href: e.href || "",
    text: e.innerText || ""

}))

""")

        print(f"🔗 TOTAL LINKS = {len(links)}")

        # =================================================
        # LOOP LINKS
        # =================================================

        for i, item in enumerate(links):

            try:

                print("")
                print("------------------------------------------------")
                print(f"🔎 LINK {i+1}/{len(links)}")
                print("------------------------------------------------")

                url = clean(item["href"])
                txt = clean(item["text"])

                print(f"🌐 URL = {url[:120]}")
                print(f"📝 TXT = {txt[:200]}")

                # =========================================
                # URL VIDE
                # =========================================

                if not url:

                    print("⛔ URL VIDE")

                    continue

                # =========================================
                # DOMAIN
                # =========================================

                if "avoventes.fr" not in url:

                    print("⛔ DOMAINE EXCLU")

                    continue

                # =========================================
                # BAD URLS
                # =========================================

                bad = [

                    "facebook",
                    "twitter",
                    "linkedin",
                    "instagram",
                    "friendlycaptcha",
                    "/contact",
                    "/mentions",
                    "/politique",
                    "/recherche"

                ]

                if any(x in url.lower() for x in bad):

                    print("⛔ URL FILTREE")

                    continue

                # =========================================
                # TEXTE
                # =========================================

                txt_low = txt.lower()

                keywords = [

                    "appartement",
                    "maison",
                    "villa",
                    "terrain",
                    "immeuble",
                    "studio",
                    "garage",
                    "local"

                ]

                if not any(k in txt_low for k in keywords):

                    print("⛔ PAS MOT CLE")

                    continue

                # =========================================
                # LONGUEUR
                # =========================================

                if len(txt) < 40:

                    print("⛔ TEXTE TROP COURT")

                    continue

                # =========================================
                # DUPLICATE
                # =========================================

                if url in seen:

                    print("⛔ DUPLICATE")

                    continue

                seen.add(url)

                # =========================================
                # ROW
                # =========================================

                row = {

                    "url": url,

                    "prix": extract_price(txt),

                    "cp": extract_cp(txt),

                    "type": detect_type(txt),

                    "txt": txt[:3000]

                }

                rows.append(row)

                print("")
                print("✅ ANNONCE VALIDEE")
                print(f"🏠 TYPE = {row['type']}")
                print(f"💰 PRIX = {row['prix']}")
                print(f"📍 CP = {row['cp']}")
                print(f"🌐 URL = {row['url']}")

            except Exception as e:

                print("")
                print("❌ ERREUR LINK")
                print(e)

        # =================================================
        # CLOSE
        # =================================================

        print("")
        print("================================================")
        print("❎ FERMETURE NAVIGATEUR")
        print("================================================")

        await browser.close()

    # =====================================================
    # DF
    # =====================================================

    print("")
    print("================================================")
    print("📦 DATAFRAME")
    print("================================================")

    df = pd.DataFrame(rows)

    if len(df) > 0:

        df = df.drop_duplicates(
            subset=["url"]
        )

    print(df.head())

    print("")
    print(f"📦 TOTAL = {len(df)}")

    return df


# =========================================================
# MAIN
# =========================================================

async def main():

    try:

        print("")
        print("================================================")
        print("🚀 MAIN")
        print("================================================")

        df = await scrape()

        # =================================================
        # EMPTY
        # =================================================

        if len(df) == 0:

            print("")
            print("================================================")
            print("❌ AUCUNE ANNONCE")
            print("================================================")

            send_telegram(
                "❌ AVOVENTES : aucune annonce"
            )

            return

        # =================================================
        # CSV
        # =================================================

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

        # =================================================
        # GEO
        # =================================================

        df = geolocate(df)

        # =================================================
        # MAP
        # =================================================

        create_map(df)

        # =================================================
        # TELEGRAM
        # =================================================

        send_telegram(
            f"✅ AVOVENTES\n{len(df)} annonces"
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
