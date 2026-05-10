# =========================================================
# VIAGER SCRAPER COMPLET
# VERSION SANS BLEU LEAFLET
# =========================================================

import asyncio
import pandas as pd
import re
import folium
import os
import requests

from playwright.async_api import async_playwright

from folium.features import DivIcon


URL = "https://www.costes-viager.com/acheter/annonces"

HISTORY_FILE = "historique_ids.csv"


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram(message):

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("❌ TOKEN ou CHAT_ID manquant")
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
# SCRAPING
# =========================================================

async def scrape():

    rows = []

    seen_urls = set()

    if os.path.exists(HISTORY_FILE):

        old = pd.read_csv(HISTORY_FILE)

        old_urls = set(old["url"])

    else:

        old_urls = set()

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=True
        )

        page = await browser.new_page()

        await page.goto(
            URL,
            timeout=60000
        )

        # cookies
        try:

            btn = await page.wait_for_selector(
                "button:has-text('Accepter')",
                timeout=5000
            )

            await btn.click()

        except:
            pass

        await page.wait_for_selector(
            "rc-card-annonce"
        )

        while True:

            cards = await page.query_selector_all(
                "rc-card-annonce"
            )

            print(f"🧩 {len(cards)} cartes analysées")

            for card in cards:

                try:

                    a = await card.query_selector("a")

                    href = (
                        await a.get_attribute("href")
                        if a else ""
                    )

                    url = (
                        "https://www.costes-viager.com" + href
                        if href else ""
                    )

                    if not url:
                        continue

                    if url in seen_urls:
                        continue

                    seen_urls.add(url)

                    if url in old_urls:

                        print("🛑 STOP (ancienne annonce)")

                        await browser.close()

                        return pd.DataFrame(
                            rows,
                            columns=[
                                "html",
                                "txt",
                                "url"
                            ]
                        )

                    html = await card.inner_html()

                    txt = await card.inner_text()

                    rows.append({

                        "html": html.strip(),

                        "txt": txt.strip(),

                        "url": url

                    })

                except Exception as e:

                    print("❌ erreur annonce", e)

            btn = await page.query_selector(
                "button:has-text('Afficher plus de résultats')"
            )

            if not btn:
                break

            await page.evaluate(
                "(b) => b.click()",
                btn
            )

            await page.wait_for_timeout(3000)

        await browser.close()

    return pd.DataFrame(
        rows,
        columns=[
            "html",
            "txt",
            "url"
        ]
    )


# =========================================================
# CLEAN
# =========================================================

def clean(txt):

    if pd.isna(txt):
        return ""

    return (
        txt
        .replace("\u202f", " ")
        .replace("\xa0", " ")
        .replace("\n", " ")
        .strip()
    )


# =========================================================
# PROCESS
# =========================================================

def process(df):

    if len(df) == 0:
        return df

    df["txt"] = df["txt"].apply(clean)

    def extract_money(label, txt):

        pattern = (
            label +
            r".*?([\d\s]+)\s?€"
        )

        m = re.search(
            pattern,
            txt,
            re.I
        )

        if not m:
            return None

        value = m.group(1)

        digits = re.sub(
            r"[^\d]",
            "",
            value
        )

        if digits:
            return int(digits)

        return None

    df["bouquet"] = df["txt"].apply(
        lambda x: extract_money(
            "Bouquet",
            x
        )
    )

    df["rente"] = df["txt"].apply(
        lambda x: extract_money(
            "Rente",
            x
        )
    )

    def extract_age(txt):

        ages = re.findall(
            r"(\d{2})\s*ans",
            txt,
            re.I
        )

        if not ages:
            return None

        ages = [int(x) for x in ages]

        return max(ages)

    df["age"] = df["txt"].apply(
        extract_age
    )

    df["cp"] = df["txt"].str.extract(
        r"\((\d{5})\)"
    )

    return df


# =========================================================
# CARTE
# =========================================================

def create_map(valid_df, rejected_df):

    m = folium.Map(
        location=[46.5, 2.5],
        zoom_start=6
    )

    # =====================================================
    # VALIDES
    # =====================================================

    for _, row in valid_df.dropna(
        subset=["lat"]
    ).iterrows():

        txt = str(
            row.get("txt", "")
        ).lower()

        age = row.get("age")

        color = "green"

        if pd.notna(age):

            age = int(age)

            if age < 72:
                color = "#000000"

            elif age < 78:
                color = "#1e8e3e"

            elif age < 84:
                color = "#ff9800"

            else:
                color = "#d32f2f"

        symbol = "🏠"

        if "appartement" in txt:
            symbol = "🏢"

        popup = (
            "<b>✅ ANNONCE VALIDEE</b><br><br>"
            f"👴 Age : {row.get('age')} ans<br>"
            f"💰 Bouquet : {row.get('bouquet')} €<br>"
            f"📆 Rente : {row.get('rente')} €/mois<br>"
            f"📍 CP : {row.get('cp')}<br><br>"
            f"<a href='{row['url']}' target='_blank'>"
            "Voir annonce"
            "</a>"
        )

        # =================================================
        # HTML CUSTOM
        # =================================================

        html = f"""
        <div style="
            width:40px;
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
                font-size:16px;
                border:2px solid white;
                box-shadow:0 0 4px rgba(0,0,0,0.5);
                margin:0;
                padding:0;
            ">
                {symbol}
            </div>

        </div>
        """

        # =================================================
        # MARKER
        # =================================================

        folium.Marker(

            [row["lat"], row["lon"]],

            popup=folium.Popup(
                popup,
                max_width=300
            ),

            icon=DivIcon(

                icon_size=(40, 40),

                icon_anchor=(20, 20),

                class_name="empty",

                html=html

            )

        ).add_to(m)

    # =====================================================
    # REJETEES
    # =====================================================

    for _, row in rejected_df.dropna(
        subset=["lat"]
    ).iterrows():

        popup = (
            "<b>❌ REJETEE</b><br><br>"
            f"👴 Age : {row.get('age')} ans<br>"
            f"💰 Bouquet : {row.get('bouquet')} €<br>"
            f"📆 Rente : {row.get('rente')} €/mois<br>"
            f"📍 CP : {row.get('cp')}<br><br>"
            f"<a href='{row['url']}' target='_blank'>"
            "Voir annonce"
            "</a>"
        )

        html = """
        <div style="
            width:40px;
            display:flex;
            align-items:center;
            justify-content:center;
        ">

            <div style="
                background:#9e9e9e;
                width:38px;
                height:38px;
                border-radius:50%;
                display:flex;
                align-items:center;
                justify-content:center;
                color:white;
                font-size:18px;
                border:2px solid white;
                box-shadow:0 0 4px rgba(0,0,0,0.5);
            ">
                ❌
            </div>

        </div>
        """

        folium.Marker(

            [row["lat"], row["lon"]],

            popup=folium.Popup(
                popup,
                max_width=300
            ),

            icon=DivIcon(

                icon_size=(40, 40),

                icon_anchor=(20, 20),

                class_name="empty",

                html=html

            )

        ).add_to(m)

    m.save("carte.html")

    print("✅ CARTE SAUVEGARDEE")


# =========================================================
# MAIN
# =========================================================

async def main():

    print("🚀 SCRAPING...")

    df = await scrape()

    print("📊 EXTRACTION...")

    df = process(df)

    if len(df) == 0:

        send_telegram(
            "😴 Aucune nouvelle annonce"
        )

        return

    # =====================================================
    # FILTRES
    # =====================================================

    filtre_vendu = df["txt"].str.contains(
        "vendu",
        case=False,
        na=False
    )

    filtre_femme = df["txt"].str.contains(
        r"Femme\s*,?\s*\d+\s*ans",
        regex=True,
        case=False,
        na=False
    )

    filtre_couple = df["txt"].str.contains(
        r"Femme.*Homme|Homme.*Femme",
        regex=True,
        case=False,
        na=False
    )

    filtre_rente = (
        df["rente"].fillna(0) > 500
    )

    filtre_bouquet = (
        df["bouquet"].fillna(0) > 150000
    )

    rejected = df[
        filtre_vendu
        |
        filtre_femme
        |
        filtre_couple
        |
        filtre_rente
        |
        filtre_bouquet
    ].copy()

    valid = df[
        ~df["url"].isin(
            rejected["url"]
        )
    ].copy()

    print(f"✅ conservées : {len(valid)}")

    print(f"❌ rejetées : {len(rejected)}")

    # =====================================================
    # GEO
    # =====================================================

    geo = pd.read_csv(
        "base-officielle-codes-postaux.csv"
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

    valid["cp"] = valid["cp"].astype(str)

    rejected["cp"] = rejected["cp"].astype(str)

    valid = valid.merge(
        geo,
        on="cp",
        how="left"
    )

    rejected = rejected.merge(
        geo,
        on="cp",
        how="left"
    )

    # =====================================================
    # HISTORIQUE
    # =====================================================

    if os.path.exists(HISTORY_FILE):

        old = pd.read_csv(HISTORY_FILE)

        old_urls = set(old["url"])

    else:

        old_urls = set()

    new_valid = valid[
        ~valid["url"].isin(old_urls)
    ]

    combined = pd.concat([
        pd.DataFrame({
            "url": list(old_urls)
        }),
        valid[["url"]]
    ]).drop_duplicates()

    combined.to_csv(
        HISTORY_FILE,
        index=False
    )

    # =====================================================
    # TELEGRAM
    # =====================================================

    if len(new_valid) > 0:

        send_telegram(
            f"🔥 {len(new_valid)} nouvelles annonces"
        )

        for _, row in new_valid.iterrows():

            msg = ""

            msg += "🏠 VIAGER\n\n"

            msg += f"👴 Age : {row.get('age')}\n"

            msg += f"💰 Bouquet : {row.get('bouquet')} €\n"

            msg += f"📆 Rente : {row.get('rente')} €/mois\n"

            msg += f"📍 CP : {row.get('cp')}\n\n"

            msg += f"🔗 {row['url']}"

            send_telegram(msg)

    else:

        send_telegram(
            "😴 Aucune nouvelle annonce"
        )

    # =====================================================
    # CARTE
    # =====================================================

    create_map(
        valid,
        rejected
    )

    send_file("carte.html")

    print("✅ FIN")


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
