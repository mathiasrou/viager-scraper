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


def send_file(path):
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{token}/sendDocument"

    with open(path, "rb") as f:
        requests.post(url, data={"chat_id": chat_id}, files={"document": f})


# =========================
# SCRAPING
# =========================
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
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(URL, timeout=60000)

        try:
            btn = await page.wait_for_selector("button:has-text('Accepter')", timeout=5000)
            await btn.click()
        except:
            pass

        await page.wait_for_selector("rc-card-annonce")

        while True:
            cards = await page.query_selector_all("rc-card-annonce")

            print(f"🧩 {len(cards)} cartes analysées")

            for card in cards:
                a = await card.query_selector("a")
                href = await a.get_attribute("href") if a else ""
                url = "https://www.costes-viager.com" + href if href else ""

                if url in seen_urls:
                    continue
                seen_urls.add(url)

                # STOP dès qu'on voit une ancienne annonce
                if url in old_urls:
                    print("🛑 STOP (ancienne annonce)")
                    await browser.close()
                    return pd.DataFrame(rows)

                html = await card.inner_html()

                rows.append({
                    "html": html.strip(),
                    "url": url
                })

            btn = await page.query_selector("button:has-text('Afficher plus de résultats')")
            if not btn:
                break

            await page.evaluate("(b) => b.click()", btn)
            await page.wait_for_timeout(800)

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
                value = m.group(1)
                if not value:
                    continue
                digits = re.sub(r"[^\d]", "", value)
                if digits:
                    return int(digits)
        return None

    df["bouquet"] = df["txt"].apply(lambda x: extract_label(["Bouquet"], x))
    df["rente"] = df["txt"].apply(lambda x: extract_label(["Rente", "Mensual"], x))

    def extract_age(txt):
        ages = re.findall(r"(\d{2})\s*ans", txt)
        if not ages:
            return None
        return int(max(ages))

    df["age"] = df["txt"].apply(extract_age)
    df["cp"] = df["txt"].str.extract(r"\((\d{5})\)")

    return df


# =========================
# FILTRES + GEO
# =========================
def enrich(df):

    df = df[~df["txt"].str.contains("vendu", case=False, na=False)]

    # supprimer femme + couple
    df = df[~df["txt"].str.contains(r"H\s*\d+\s*ans.*F\s*\d+\s*ans|F\s*\d+\s*ans.*H\s*\d+\s*ans", regex=True, na=False)]

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
    new_df = new_df.drop_duplicates(subset=["url"])

    # sauvegarde historique
    if os.path.exists(HISTORY_FILE):
        combined = pd.concat([old, df[["url"]]]).drop_duplicates()
    else:
        combined = df[["url"]]

    combined.to_csv(HISTORY_FILE, index=False)

    print(f"🆕 {len(new_df)} nouvelles annonces")

    if len(new_df) > 0:
        send_telegram(f"🔥 {len(new_df)} nouvelles annonces")
    
        for i, row in new_df.iterrows():
    
            # limite à 3 annonces pour éviter spam
            if i >= 3:
                break
    
            message = "🔍 DEBUG ANNONCE\n"
            message += f"🔗 {row['url']}\n\n"
    
            # HTML brut
            if pd.notna(row.get("html")):
                html_preview = row["html"][:1000].replace("\n", " ")
                message += f"🧩 HTML:\n{html_preview}\n\n"
    
            # Texte nettoyé
            if pd.notna(row.get("txt")):
                txt_preview = row["txt"][:1000].replace("\n", " ")
                message += f"🧾 TXT:\n{txt_preview}\n\n"
    
            send_telegram(message)
    
    else:
        send_telegram("😴 Aucune nouvelle annonce")

    # carte uniquement
    create_map(df)
    send_file("carte.html")

    print("✅ FIN")


# =========================
if __name__ == "__main__":
    asyncio.run(main())
