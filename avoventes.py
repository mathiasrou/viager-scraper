# =========================================================
# AVOVENTES SCRAPER + HISTORIQUE + CARTE
# VERSION COMPLETE
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

    if "maison" in t:
        return "Maison"

    if "villa" in t:
        return "Villa"

    if "terrain" in t:
        return "Terrain"

    if "immeuble" in t:
        return "Immeuble"

    if "local commercial" in t:
        return "Local commercial"

    return "Autre"


# =========================================================
# EXTRACTIONS
# =========================================================

def extract_price(txt):

    try:

        m = re.search(
            r"Mise à prix(?: initiale)?\s*:\s*([\d\s]+)",
            txt,
            re.I
        )

        if not m:
            return None

        val = (
            m.group(1)
            .replace(" ", "")
        )

        return int(val)

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


def extract_date(txt):

    try:

        m = re.search(
            r"Date de la vente\s*:\s*(.+?)(?:Date des visites|$)",
            txt,
            re.S | re.I
        )

        if not m:
            return None

        full = clean(m.group(1))

        d = re.search(
            r"(\d{2})\s+(\w+)\s+(\d{4})",
            full,
            re.I
        )

        if not d:
            return None

        mois = {
            "janvier": "01",
            "février": "02",
            "mars": "03",
            "avril": "04",
            "mai": "05",
            "juin": "06",
            "juillet": "07",
            "août": "08",
            "septembre": "09",
            "octobre": "10",
            "novembre": "11",
            "décembre": "12"
        }

        jour = d.group(1)

        mois_txt = d.group(2).lower()

        annee = d.group(3)

        if mois_txt not in mois:
            return None

        return f"{jour}/{mois[mois_txt]}/{annee}"

    except:
        return None


def extract_status(txt):

    t = txt.lower()

    if "adjugé" in t:
        return "terminee"

    if "retirée" in t:
        return "retiree"

    if "reportée" in t:
        return "reportee"

    return "future"


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
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage"
            ]
        )

        page = await browser.new_page(
            viewport={
                "width": 1600,
                "height": 4000
            }
        )

        print("🌐 OUVERTURE")

        await page.goto(
            BASE_URL,
            wait_until="networkidle",
            timeout=120000
        )

        # =====================================================
        # COOKIES
        # =====================================================

        try:

            await page.locator(
                "button:has-text('Tout accepter')"
            ).click(
                timeout=5000
            )

            print("🍪 COOKIES OK")

            await page.wait_for_timeout(3000)

        except Exception as e:

            print("❌ COOKIE")
            print(e)

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
        # TEXTE
        # =====================================================

        body = await page.locator("body").inner_text()

        blocs = re.split(
            r"Vente aux enchères",
            body
        )

        print(f"📦 BLOCS = {len(blocs)}")

        for bloc in blocs:

            bloc = clean(bloc)

            if len(bloc) < 100:
                continue

            try:

                row = {}

                row["txt"] = bloc

                row["type"] = detect_type(bloc)

                row["prix"] = extract_price(bloc)

                row["cp"] = extract_cp(bloc)

                row["date_vente"] = extract_date(bloc)

                row["status"] = extract_status(bloc)

                # =================================================
                # TITRE
                # =================================================

                titre = ""

                lignes = bloc.split("Date de la vente")[0]

                morceaux = lignes.split("France")

                if len(morceaux) > 0:

                    titre = morceaux[0][-150:]

                row["titre"] = clean(titre)

                # =================================================
                # URL
                # =================================================

                row["url"] = BASE_URL

                key = (
                    str(row["titre"])
                    + str(row["prix"])
                )

                if key in seen:
                    continue

                seen.add(key)

                rows.append(row)

                print(
                    f"✅ {row['type']} | "
                    f"{row['prix']}€ | "
                    f"{row['cp']}"
                )

            except Exception as e:

                print("❌ BLOC")
                print(e)

        await browser.close()

    df = pd.DataFrame(rows)

    # =========================================================
    # HISTORIQUE
    # =========================================================

    if len(old_df) > 0:

        df = pd.concat(
            [old_df, df],
            ignore_index=True
        )

    if len(df) > 0:

        df = df.drop_duplicates(
            subset=["titre", "prix"],
            keep="first"
        )

    return df


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
# CREATE MAP
# =========================================================

def create_map(df):

    active_df = df.copy()

    active_df = active_df.drop_duplicates(
        subset=["titre", "prix"]
    )

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

                    d = datetime.strptime(
                        row["date_vente"],
                        "%d/%m/%Y"
                    )

                    delta = (
                        d - datetime.now()
                    ).days

                    bottom = f"{delta}j"

            except:
                pass

            popup = f"""
<b>{row['type']}</b><br><br>

💰 Prix : {row['prix']} €<br>

📅 Vente : {row['date_vente']}<br>

📍 CP : {row['cp']}<br><br>

📝 {row['titre']}<br><br>

<a href="{row['url']}" target="_blank">
Voir annonce
</a>
"""

            html = f"""
<div style="width:42px; display:flex; flex-direction:column; align-items:center; justify-content:center; background:transparent;">
    <div style="background:{color}; width:38px; height:38px; border-radius:50%; display:flex; align-items:center; justify-content:center; color:white; font-weight:bold; font-size:14px; border:2px solid white; box-shadow:0 0 4px rgba(0,0,0,0.4);">
        {symbol}
    </div>
    <div style="font-size:11px; font-weight:bold; color:black; background:white; padding:1px 4px; border-radius:6px; margin-top:2px; border:1px solid #999; white-space:nowrap;">
        {bottom}
    </div>
</div>
"""

            marker = folium.Marker(
                location=[row["lat"], row["lon"]],
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

        print(f"📦 TOTAL = {len(df)}")

        if len(df) == 0:
            return

        df.to_csv(
            HISTORY_FILE,
            sep=";",
            index=False,
            encoding="utf-8-sig"
        )

        print("💾 HISTORIQUE OK")

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
            f"❌ ERREUR\n{e}"
        )


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
