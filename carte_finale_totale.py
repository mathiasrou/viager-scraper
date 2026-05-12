import asyncio
import pandas as pd
import re
import folium
import os
import requests

from playwright.async_api import async_playwright


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

    # historique
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

                    # doublons
                    if url in seen_urls:
                        continue

                    seen_urls.add(url)

                    # stop historique
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

            # bouton afficher plus
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
# NETTOYAGE
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
# EXTRACTION
# =========================================================
def process(df):

    if len(df) == 0:
        return df

    df["txt"] = df["txt"].apply(clean)

    # =========================
    # EXTRACTION ARGENT
    # =========================
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

    # bouquet
    df["bouquet"] = df["txt"].apply(
        lambda x: extract_money(
            "Bouquet",
            x
        )
    )

    # rente
    df["rente"] = df["txt"].apply(
        lambda x: extract_money(
            "Rente",
            x
        )
    )

    # =========================
    # AGE
    # =========================
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

    # =========================
    # CP
    # =========================
    df["cp"] = df["txt"].str.extract(
        r"\((\d{5})\)"
    )

    return df


# =========================================================
# DEBUG
# =========================================================
def send_debug(row):

    txt = row["txt"]

    age_matches = re.findall(
        r"(\d{2})\s*ans",
        txt,
        re.I
    )

    bouquet_match = re.search(
        r"Bouquet.*?([\d\s]+)\s?€",
        txt,
        re.I
    )

    rente_match = re.search(
        r"Rente.*?([\d\s]+)\s?€",
        txt,
        re.I
    )

    cp_match = re.search(
        r"\((\d{5})\)",
        txt
    )

    debug = ""

    debug += "🔍 DEBUG EXTRACTION\n\n"

    debug += f"🔗 URL :\n{row['url']}\n\n"

    debug += "🧾 TEXTE SOURCE :\n"
    debug += txt[:3000]
    debug += "\n\n"

    debug += "====================\n"
    debug += "🎯 RESULTATS EXTRAITS\n"
    debug += "====================\n\n"

    debug += f"👴 AGE FINAL = {row.get('age')}\n"
    debug += f"➡️ MATCHES AGE = {age_matches}\n\n"

    debug += f"💰 BOUQUET FINAL = {row.get('bouquet')}\n"

    if bouquet_match:
        debug += f"➡️ MATCH BOUQUET = {bouquet_match.group(0)}\n\n"
    else:
        debug += "➡️ MATCH BOUQUET = AUCUN\n\n"

    debug += f"📆 RENTE FINAL = {row.get('rente')}\n"

    if rente_match:
        debug += f"➡️ MATCH RENTE = {rente_match.group(0)}\n\n"
    else:
        debug += "➡️ MATCH RENTE = AUCUN\n\n"

    debug += f"📍 CP FINAL = {row.get('cp')}\n"

    if cp_match:
        debug += f"➡️ MATCH CP = {cp_match.group(0)}\n\n"
    else:
        debug += "➡️ MATCH CP = AUCUN\n\n"

    send_telegram(debug[:4000])


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
                color = "black"

            elif age < 78:
                color = "green"

            elif age < 84:
                color = "orange"

            else:
                color = "red"

        icon_name = "home"

        if "appartement" in txt:
            icon_name = "building"

        elif "maison" in txt:
            icon_name = "home"

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

        folium.Marker(

            [row["lat"], row["lon"]],

            popup=folium.Popup(
                popup,
                max_width=300
            ),

            icon=folium.Icon(
                color=color,
                icon=icon_name,
                prefix="fa"
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

        folium.Marker(

            [row["lat"], row["lon"]],

            popup=folium.Popup(
                popup,
                max_width=300
            ),

            icon=folium.Icon(
                color="lightgray",
                icon="remove",
                prefix="fa"
            )

        ).add_to(m)

    m.save("carte.html")


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

    # DEBUG
    send_debug(df.iloc[0])

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
            "😴 René Costes - pas de nouvelles annoncdes"
        )

    # =====================================================
    # CARTE
    # =====================================================
    create_map(
        valid,
        rejected
    )

    send_file("carte_rene_costes.html")

    print("✅ FIN")


# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":

    asyncio.run(main())
