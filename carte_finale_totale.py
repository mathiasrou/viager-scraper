# -*- coding: utf-8 -*-
"""
Created on Wed Apr 15 10:49:11 2026


@author: M_a_t
"""
import requests
import os
import asyncio
import pandas as pd
import re
import folium
from playwright.async_api import async_playwright, TimeoutError

URL = "https://www.costes-viager.com/acheter/annonces"

# =========================
# SCRAPING
# =========================
def send_telegram(message, file_path=None):

    TOKEN = os.getenv("TELEGRAM_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    if not TOKEN or not CHAT_ID:
        print("❌ TOKEN ou CHAT_ID manquant")
        return

    # message texte
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, data={
        "chat_id": CHAT_ID,
        "text": message
    })

    # fichier (carte)
    if file_path:
        url_file = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
        with open(file_path, "rb") as f:
            requests.post(url_file, files={"document": f}, data={"chat_id": CHAT_ID})

    print("📩 Telegram envoyé")


async def scrape():
    rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(URL, timeout=60000)

        # Cookies
        try:
            btn = await page.wait_for_selector("button:has-text('Accepter')", timeout=5000)
            await btn.click()
            print("🍪 Cookies OK")
        except TimeoutError:
            print("🍪 Pas de bandeau cookies")

        await page.wait_for_selector("rc-card-annonce")

        # Charger toutes les annonces
        prev = 0
        while True:
            cards = await page.query_selector_all("rc-card-annonce")
            cur = len(cards)

            print(f"🧩 {cur} cartes")

            if cur == prev:
                break

            prev = cur

            btn = await page.query_selector("button:has-text('Afficher plus de résultats')")
            if not btn:
                break

            await page.evaluate("(b) => b.click()", btn)
            await page.wait_for_timeout(800)

        # Extraction
        cards = await page.query_selector_all("rc-card-annonce")
        print(f"✅ Total : {len(cards)}")

        for idx, card in enumerate(cards, start=1):
            html = await card.inner_html()
            a = await card.query_selector("a")
            href = await a.get_attribute("href") if a else ""

            rows.append({
                "id": idx,
                "html": html.strip(),
                "url": "https://www.costes-viager.com" + href if href else ""
            })

        await browser.close()

    return pd.DataFrame(rows)

# =========================
# EXTRACTION
# =========================
def process(df):

    def clean(txt):
        if pd.isna(txt):
            return ""
        return txt.replace("\u202f", " ").replace("\xa0", " ")

    df["txt"] = df["html"].apply(clean)

    def extract_label(labels, txt):
        for label in labels:
            m = re.search(label + r".*?([\d\s]+)\s?€", txt, re.I)
            if m:
                digits = re.sub(r"[^\d]", "", m.group(1))
                if digits:
                    return int(digits)
        return None

    df["bouquet"] = df["txt"].apply(lambda x: extract_label(["Bouquet"], x))
    df["rente"] = df["txt"].apply(lambda x: extract_label(["Rente", "Mensual"], x))
    df["prix_achat"] = df["txt"].apply(lambda x: extract_label(["Prix"], x))
    df["valeur_bien"] = df["txt"].apply(lambda x: extract_label(["Valeur"], x))

    def extract_age(txt):
        m = re.search(r"(\d{2})\s*ans", txt)
        return int(m.group(1)) if m else None

    df["age"] = df["txt"].apply(extract_age)

    def extract_cp(txt):
        m = re.search(r"\((\d{5})\)", txt)
        return m.group(1) if m else None

    df["cp"] = df["txt"].apply(extract_cp)

    def detect_type(txt):
        txt = txt.lower()
        if "maison" in txt:
            return "maison"
        if "appartement" in txt:
            return "appartement"
        return "autre"

    df["type"] = df["txt"].apply(detect_type)

    return df

# =========================
# FILTRES + GEO
# =========================
def enrich(df):

    df = df[~df["txt"].str.contains("Femme", case=False, na=False)]
    df = df[~df["txt"].str.contains("vendu", case=False, na=False)]

    df = df[
        ((df["rente"].isna()) | (df["rente"] <= 500)) &
        ((df["bouquet"].isna()) | (df["bouquet"] <= 150000))
    ]

    geo = pd.read_csv("base-officielle-codes-postaux.csv")
    geo = geo[["code_postal", "latitude", "longitude"]]
    geo.columns = ["cp", "lat", "lon"]

    geo["cp"] = geo["cp"].astype(str)
    df["cp"] = df["cp"].astype(str)

    geo = geo.drop_duplicates(subset=["cp"])

    df = df.merge(geo, on="cp", how="left")

    return df

# =========================
# CARTE
# =========================
def create_map(df):

    def color_age(age):
        if pd.isna(age):
            return "gray"
        if age < 70:
            return "green"
        if age < 78:
            return "yellow"
        if age < 83:
            return "orange"
        return "red"

    def euro(x):
        if pd.isna(x):
            return "-"
        return f"{int(x):,}".replace(",", " ") + " €"

    m = folium.Map(location=[46.5, 2.5], zoom_start=6)

    for _, row in df.dropna(subset=["lat"]).iterrows():

        popup = f"""
        <b>{row['type']}</b><br>
        Âge : {row['age']}<br>
        Bouquet : {euro(row['bouquet'])}<br>
        Rente : {euro(row['rente'])}<br>
        <a href="{row['url']}" target="_blank">Voir</a>
        """

        folium.Marker(
            [row["lat"], row["lon"]],
            popup=popup,
            icon=folium.Icon(color=color_age(row["age"]))
        ).add_to(m)

    m.save("carte_finale_totale.html")
    print("✅ Carte générée")

# =========================
# MAIN
# =========================
async def main():

    print("🚀 SCRAPING...")
    df = await scrape()

    print("📊 EXTRACTION...")
    df = process(df)

    print("🧠 FILTRES + GEO...")
    df = enrich(df)

    print("🗺️ CARTE...")
    create_map(df)

    df.to_csv("resultat_final.csv", index=False)

    print(f"✅ FIN : {len(df)} annonces exploitables")
    send_telegram(
    f"✅ Scraping terminé\n{len(df)} annonces exploitables",
    "carte_finale_totale.html"
)
# =========================
# EXECUTION
# =========================
if __name__ == "__main__":
    asyncio.run(main())
