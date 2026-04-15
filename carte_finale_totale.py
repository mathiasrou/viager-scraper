import asyncio
import pandas as pd
import re
import folium
import os
import requests
from playwright.async_api import async_playwright, TimeoutError

URL = "https://www.costes-viager.com/acheter/annonces"
HISTORY_FILE = "historique_ids.csv"

# =========================
# TELEGRAM
# =========================
def send_telegram(message):
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("❌ TOKEN ou CHAT_ID manquant")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    requests.post(url, data={
        "chat_id": chat_id,
        "text": message
    })

# =========================
# SCRAPING
# =========================
async def scrape():
    rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(URL, timeout=60000)

        try:
            btn = await page.wait_for_selector("button:has-text('Accepter')", timeout=5000)
            await btn.click()
        except TimeoutError:
            pass

        await page.wait_for_selector("rc-card-annonce")

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

        cards = await page.query_selector_all("rc-card-annonce")

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

    df["txt"] = df["html"].str.replace("\u202f", " ").str.replace("\xa0", " ")

    def extract(label, txt):
        m = re.search(label + r".*?([\d\s]+)\s?€", txt, re.I)
        if m:
            return int(re.sub(r"[^\d]", "", m.group(1)))
        return None

    df["bouquet"] = df["txt"].apply(lambda x: extract("Bouquet", x))
    df["rente"] = df["txt"].apply(lambda x: extract("Rente|Mensual", x))
    df["age"] = df["txt"].str.extract(r"(\d{2})\s*ans").astype(float)
    df["cp"] = df["txt"].str.extract(r"\((\d{5})\)")

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

    df["cp"] = df["cp"].astype(str)
    geo["cp"] = geo["cp"].astype(str)

    return df.merge(geo, on="cp", how="left")

# =========================
# MAP
# =========================
def create_map(df):

    m = folium.Map(location=[46.5, 2.5], zoom_start=6)

    for _, row in df.dropna(subset=["lat"]).iterrows():
        folium.Marker(
            [row["lat"], row["lon"]],
            popup=row["url"],
            icon=folium.Icon(color="green")
        ).add_to(m)

    m.save("carte.html")

# =========================
# MAIN
# =========================
async def main():

    print("🚀 SCRAPING...")
    df = await scrape()

    print("📊 EXTRACTION...")
    df = process(df)

    print("🧠 FILTRES...")
    df = enrich(df)

    # historique
    if os.path.exists(HISTORY_FILE):
        old = pd.read_csv(HISTORY_FILE)
        old_ids = set(old["url"])
    else:
        old_ids = set()

    new_df = df[~df["url"].isin(old_ids)]

    # save historique
    df[["url"]].to_csv(HISTORY_FILE, index=False)

    print(f"🆕 {len(new_df)} nouvelles annonces")

    if len(new_df) > 0:
        send_telegram(f"🔥 {len(new_df)} nouvelles annonces !")

        for _, row in new_df.head(10).iterrows():
            send_telegram(f"{row['age']} ans\n{row['url']}")
    else:
        send_telegram("😴 Aucune nouvelle annonce")

    create_map(df)

    print("✅ FIN")

# =========================
if __name__ == "__main__":
    asyncio.run(main())
