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

BASE_URL = (
    "https://www.vench.fr/"
    "prochaines-ventes-aux-encheres.html"
)

CSV_CP = "base-officielle-codes-postaux.csv"

HISTORY_FILE = "historique_vench.csv"

OUTPUT_MAP = "carte_vench.html"


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

        url = f"https://api.telegram.org/bot{token}/sendMessage"

        requests.post(
            url,
            data={
                "chat_id": chat_id,
                "text": message
            },
            timeout=30
        )

    except Exception as e:

        print("❌ TELEGRAM")
        print(e)


def send_file(path):

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        return

    try:

        url = f"https://api.telegram.org/bot{token}/sendDocument"

        with open(path, "rb") as f:

            requests.post(
                url,
                data={"chat_id": chat_id},
                files={"document": f},
                timeout=60
            )

    except Exception as e:

        print("❌ TELEGRAM FILE")
        print(e)


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

        matches = re.findall(
            r"(\d[\d\s]+(?:[\.,]\d+)?)\s*€",
            txt
        )

        vals = []

        for ptxt in matches:

            try:

                v = (
                    ptxt
                    .replace(" ", "")
                    .replace(",", ".")
                )

                v = v.split(".")[0]

                v = int(v)

                if 1000 <= v <= 100000000:
                    vals.append(v)

            except:
                pass

        if len(vals) == 0:
            return None

        return min(vals)

    except:
        return None


def extract_cp(txt):

    try:

        m = re.findall(r"\b(\d{5})\b", txt)

        if len(m) > 0:
            return m[0]

    except:
        pass

    return None


def extract_date(txt):

    try:

        m = re.search(
            r"(\d{2}/\d{2}/\d{4})",
            txt
        )

        if m:
            return m.group(1)

    except:
        pass

    return None


def extract_surface(txt):

    try:

        m = re.search(
            r"(\d+(?:[\.,]\d+)?)\s?m²",
            txt,
            re.I
        )

        if m:
            return m.group(1)

    except:
        pass

    return None


def extract_tribunal(txt):

    try:

        m = re.search(
            r"Tribunal(?: judiciaire)? de ([^\n]+)",
            txt,
            re.I
        )

        if m:
            return clean(m.group(1))

    except:
        pass

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
# SCRAPE VENCH
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

        context = await browser.new_context(
            viewport={
                "width": 1600,
                "height": 4000
            }
        )

        page = await context.new_page()

        # =====================================================
        # PAGES
        # =====================================================

        for page_num in range(1, 6):

            try:

                if page_num == 1:

                    url = BASE_URL

                else:

                    url = (
                        BASE_URL
                        + f"?p={page_num}"
                    )

                print("")
                print("=" * 80)
                print(f"📄 PAGE {page_num}")
                print(url)
                print("=" * 80)

                await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=120000
                )

                await page.wait_for_timeout(5000)

                # =================================================
                # DEBUG
                # =================================================

                await page.screenshot(
                    path=f"debug_vench_page_{page_num}.png",
                    full_page=True
                )

                html = await page.content()

                with open(
                    f"debug_vench_page_{page_num}.html",
                    "w",
                    encoding="utf-8"
                ) as f:

                    f.write(html)

                # =================================================
                # LINKS
                # =================================================

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

                print("")
                print(f"🏠 {len(links)} annonces")

                # =================================================
                # DETAILS
                # =================================================

                for i, detail_url in enumerate(links):

                    try:

                        print("")
                        print("=" * 80)
                        print(
                            f"🏠 DETAIL "
                            f"{i+1}/{len(links)}"
                        )
                        print(detail_url)
                        print("=" * 80)

                        detail = await context.new_page()

                        await detail.goto(
                            detail_url,
                            wait_until="domcontentloaded",
                            timeout=120000
                        )

                        await detail.wait_for_timeout(3000)

                        await detail.screenshot(
                            path=f"detail_{page_num}_{i}.png",
                            full_page=True
                        )

                        txt = await detail.locator(
                            "body"
                        ).inner_text()

                        txt = clean(txt)

                        title = await detail.title()

                        full = title + "\n" + txt

                        with open(
                            f"detail_{page_num}_{i}.txt",
                            "w",
                            encoding="utf-8"
                        ) as f:

                            f.write(full)

                        row = {}

                        row["url"] = detail_url

                        row["type"] = detect_type(full)

                        row["prix"] = extract_price(full)

                        row["cp"] = extract_cp(full)

                        row["date_vente"] = extract_date(full)

                        row["surface"] = extract_surface(full)

                        row["tribunal"] = extract_tribunal(full)

                        row["status"] = extract_status(full)

                        # =========================================
                        # TITRE
                        # =========================================

                        try:

                            h1 = await detail.locator(
                                "h1"
                            ).inner_text()

                            row["titre"] = clean(h1)

                        except:

                            row["titre"] = title

                        # =========================================
                        # UNIQUE
                        # =========================================

                        row["id_unique"] = (
                            f"{row['url']}"
                        )

                        if row["id_unique"] in seen:
                            continue

                        seen.add(
                            row["id_unique"]
                        )

                        rows.append(row)

                        print("")
                        print("✅ EXTRACTION")
                        print(row)

                        await detail.close()

                    except Exception as e:

                        print("")
                        print("❌ DETAIL ERROR")
                        print(e)

                        traceback.print_exc()

            except Exception as e:

                print("")
                print("❌ PAGE ERROR")
                print(e)

                traceback.print_exc()

        await browser.close()

    return pd.DataFrame(rows)


# =========================================================
# GEOLOCALISATION
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

    print(
        "📍 GEOLOCALISATION OK :",
        df["lat"].notna().sum(),
        "annonces"
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

            popup = f"""
<b>{row['type']}</b><br><br>

💰 Prix : {row['prix']} €<br>

📅 Vente : {row['date_vente']}<br>

📍 CP : {row['cp']}<br>

📐 Surface : {row['surface']}<br>

🏛 Tribunal : {row['tribunal']}<br><br>

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

        print("🚀 SCRAPING VENCH")

        df = await scrape()

        print(f"📦 SCRAPE = {len(df)}")

        if len(df) == 0:

            send_telegram(
                "❌ VENCH : aucune annonce scrapee"
            )

            return

        # =====================================================
        # HISTORIQUE
        # =====================================================

        if os.path.exists(HISTORY_FILE):

            try:

                old_df = pd.read_csv(
                    HISTORY_FILE,
                    sep=";",
                    on_bad_lines="skip"
                )

            except:

                old_df = pd.DataFrame()

        else:

            old_df = pd.DataFrame()

        if "id_unique" not in old_df.columns:

            print("⚠️ HISTORIQUE CORROMPU")

            old_df = pd.DataFrame(
                columns=["id_unique"]
            )

        if len(old_df) > 0:

            old_ids = set(
                old_df["id_unique"]
                .astype(str)
            )

        else:

            old_ids = set()

        # =====================================================
        # NOUVELLES
        # =====================================================

        new_df = df[
            ~df["id_unique"]
            .astype(str)
            .isin(old_ids)
        ].copy()

        print(
            f"🆕 NOUVELLES = {len(new_df)}"
        )

        # =====================================================
        # SAVE HISTORIQUE
        # =====================================================

        combined = pd.concat(
            [old_df, df],
            ignore_index=True
        )

        combined = combined.drop_duplicates(
            subset=["id_unique"],
            keep="first"
        )

        cols = [
            "id_unique",
            "url",
            "prix",
            "cp",
            "type",
            "date_vente",
            "status",
            "titre"
        ]

        combined = combined[cols]

        combined.to_csv(
            HISTORY_FILE,
            sep=";",
            index=False,
            encoding="utf-8-sig"
        )

        print("💾 HISTORIQUE OK")

        # =====================================================
        # AUCUNE NOUVEAUTE
        # =====================================================

        if len(new_df) == 0:

            send_telegram(
                "😴 VENCH : aucune nouvelle annonce"
            )

            return

        # =====================================================
        # GEO
        # =====================================================

        new_df = geolocate(new_df)

        # =====================================================
        # MAP
        # =====================================================

        create_map(new_df)

        send_file(OUTPUT_MAP)

        # =====================================================
        # TELEGRAM
        # =====================================================

        send_telegram(
            f"🔥 VENCH\n"
            f"{len(new_df)} nouvelles annonces"
        )

        for _, row in new_df.iterrows():

            msg = ""

            msg += "🏠 ENCHERE\n\n"

            msg += f"📍 Type : {row.get('type')}\n"

            msg += f"💰 Prix : {row.get('prix')} €\n"

            msg += f"📅 Vente : {row.get('date_vente')}\n"

            msg += f"📍 CP : {row.get('cp')}\n\n"

            msg += f"{row.get('titre')}\n\n"

            msg += f"🔗 {row['url']}"

            # send_telegram(msg)

        print("✅ FIN")

    except Exception as e:

        print("❌ MAIN")
        print(e)

        traceback.print_exc()

        send_telegram(
            f"❌ ERREUR VENCH\n{e}"
        )


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
