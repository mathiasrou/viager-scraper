import asyncio
import pandas as pd
import re
import folium
import os
import requests
from playwright.async_api import async_playwright

# ========== CONFIGURATION ==========
URL = "https://www.costes-viager.com/acheter/annonces"
HISTORY_FILE = "historique_ids.csv"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ========== TELEGRAM ==========
def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ TELEGRAM_TOKEN ou CHAT_ID manquant")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message})
    except Exception as e:
        print(f"❌ Erreur envoi Telegram : {e}")

def send_file(path):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    try:
        with open(path, "rb") as f:
            requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID}, files={"document": f})
    except Exception as e:
        print(f"❌ Erreur envoi fichier : {e}")

# ========== SCRAPING ==========
async def scrape():
    rows = []
    seen_urls = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(URL, timeout=60000)

        # Accepter les cookies
        try:
            btn = await page.wait_for_selector("button:has-text('Accepter')", timeout=5000)
            await btn.click()
        except:
            pass

        await page.wait_for_selector("rc-card-annonce")

        while True:
            cards = await page.query_selector_all("rc-card-annonce")
            print(f"🧩 {len(cards)} cartes analysées sur cette page")

            for card in cards:
                try:
                    a = await card.query_selector("a")
                    href = await a.get_attribute("href") if a else ""
                    url = "https://www.costes-viager.com" + href if href else ""
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)

                    html = await card.inner_html()
                    txt = await card.inner_text()
                    rows.append({"html": html.strip(), "txt": txt.strip(), "url": url})
                except Exception as e:
                    print("❌ erreur annonce", e)

            # Tentative de charger la page suivante
            btn = await page.query_selector("button:has-text('Afficher plus')")
            if not btn:
                print("🚫 Plus de bouton 'Afficher plus' → fin du scraping")
                break

            old_count = len(cards)
            await btn.click()

            # Attendre qu'au moins une nouvelle carte apparaisse
            try:
                await page.wait_for_function(
                    """
                    (oldCount) => {
                        const cards = document.querySelectorAll('rc-card-annonce');
                        return cards.length > oldCount;
                    }
                    """,
                    arg=old_count,
                    timeout=10000
                )
                print("✅ Nouvelle page chargée")
            except:
                print("⏳ Aucune nouvelle carte après clic, on arrête")
                break

        await browser.close()

    return pd.DataFrame(rows, columns=["html", "txt", "url"])

# ========== NETTOYAGE ==========
def clean(txt):
    if pd.isna(txt):
        return ""
    return txt.replace("\u202f", " ").replace("\xa0", " ").replace("\n", " ").strip()

# ========== EXTRACTION ==========
def process(df):
    if len(df) == 0:
        return df

    df["txt"] = df["txt"].apply(clean)

    # Extraction des montants
    def extract_money(label, txt):
        pattern = label + r".*?([\d\s]+)\s?€"
        m = re.search(pattern, txt, re.I)
        if not m:
            return None
        digits = re.sub(r"[^\d]", "", m.group(1))
        return int(digits) if digits else None

    df["bouquet"] = df["txt"].apply(lambda x: extract_money("Bouquet", x))
    df["rente"] = df["txt"].apply(lambda x: extract_money("Rente", x))

    # Âge maximal (pour tous)
    def extract_age(txt):
        ages = re.findall(r"(\d{2})\s*ans", txt, re.I)
        return max(map(int, ages)) if ages else None

    df["age"] = df["txt"].apply(extract_age)

    # Âge de la femme (si présente)
    def extract_female_age(txt):
        match = re.search(r"Femme\s*,?\s*(\d{2})\s*ans", txt, re.I)
        if match:
            return int(match.group(1))
        return None

    df["femme_age"] = df["txt"].apply(extract_female_age)

    # Code postal
    df["cp"] = df["txt"].str.extract(r"\((\d{5})\)")

    return df

# ========== FILTRES ==========
def filter_annonces(df):
    # Rejet des vendues
    filtre_vendu = df["txt"].str.contains("vendu", case=False, na=False)

    # Détection de la présence d'une femme (seule ou en couple)
    filtre_femme_presente = df["txt"].str.contains(r"Femme\s*,?\s*\d+\s*ans", regex=True, case=False, na=False)

    # Pour les annonces avec femme, on garde uniquement si la femme a plus de 90 ans
    # On crée un masque pour les annonces avec femme mais âge <= 90
    filtre_femme_jeune = filtre_femme_presente & (df["femme_age"].fillna(0) <= 90)

    # Rejet des rentes > 1800
    filtre_rente = df["rente"].fillna(0) > 1800

    # Rejet des bouquets > 150000
    filtre_bouquet = df["bouquet"].fillna(0) > 150000

    # Rejet final : vendu OU (femme jeune) OU rente trop élevée OU bouquet trop élevé
    rejet = filtre_vendu | filtre_femme_jeune | filtre_rente | filtre_bouquet

    rejected = df[rejet].copy()
    valid = df[~rejet].copy()

    print(f"✅ conservées : {len(valid)}")
    print(f"❌ rejetées : {len(rejected)}")
    return valid, rejected

# ========== CARTE ==========
def create_map(valid_df, rejected_df):
    m = folium.Map(location=[46.5, 2.5], zoom_start=6)

    for _, row in valid_df.dropna(subset=["lat"]).iterrows():
        txt = str(row.get("txt", "")).lower()
        age = row.get("age")
        color = "green"
        if pd.notna(age):
            age = int(age)
            if age < 72: color = "black"
            elif age < 78: color = "green"
            elif age < 84: color = "orange"
            else: color = "red"

        icon_name = "home" if "maison" in txt else "building" if "appartement" in txt else "home"
        popup = (f"<b>✅ ANNONCE VALIDEE</b><br><br>"
                 f"👴 Age : {row.get('age')} ans<br>"
                 f"💰 Bouquet : {row.get('bouquet')} €<br>"
                 f"📆 Rente : {row.get('rente')} €/mois<br>"
                 f"📍 CP : {row.get('cp')}<br><br>"
                 f"<a href='{row['url']}' target='_blank'>Voir annonce</a>")
        folium.Marker([row["lat"], row["lon"]], popup=folium.Popup(popup, max_width=300),
                      icon=folium.Icon(color=color, icon=icon_name, prefix="fa")).add_to(m)

    for _, row in rejected_df.dropna(subset=["lat"]).iterrows():
        popup = (f"<b>❌ REJETEE</b><br><br>"
                 f"👴 Age : {row.get('age')} ans<br>"
                 f"💰 Bouquet : {row.get('bouquet')} €<br>"
                 f"📆 Rente : {row.get('rente')} €/mois<br>"
                 f"📍 CP : {row.get('cp')}<br><br>"
                 f"<a href='{row['url']}' target='_blank'>Voir annonce</a>")
        folium.Marker([row["lat"], row["lon"]], popup=folium.Popup(popup, max_width=300),
                      icon=folium.Icon(color="lightgray", icon="remove", prefix="fa")).add_to(m)

    m.save("carte_rene_costes.html")

# ========== MAIN ==========
async def main():
    print("🚀 SCRAPING...")
    df = await scrape()
    if df.empty:
        send_telegram("😴 Aucune annonce récupérée (site inaccessible ?)")
        return

    print("📊 EXTRACTION...")
    df = process(df)

    # Charger l'historique
    if os.path.exists(HISTORY_FILE):
        old = pd.read_csv(HISTORY_FILE)
        old_urls = set(old["url"])
    else:
        old_urls = set()

    # Filtrer
    valid, rejected = filter_annonces(df)

    # Nouvelles annonces (non présentes dans l'historique)
    new_valid = valid[~valid["url"].isin(old_urls)]

    # Mettre à jour l'historique
    all_urls = old_urls.union(set(valid["url"]))
    pd.DataFrame({"url": list(all_urls)}).to_csv(HISTORY_FILE, index=False)

    # Envoi Telegram : un seul message avec le nombre
    if not new_valid.empty:
        msg = f"🏠 René Costes\n{len(new_valid)} nouvelle(s) annonce(s) viager (hommes seuls ou couples avec femme > 90 ans, rente ≤1800€, bouquet ≤150k€)"
        send_telegram(msg)
        # On envoie la carte
        # Géocodage pour la carte
        geo = pd.read_csv("base-officielle-codes-postaux.csv")
        geo = geo[["code_postal", "latitude", "longitude"]]
        geo.columns = ["cp", "lat", "lon"]
        geo["cp"] = geo["cp"].astype(str)

        # On fusionne avec les données filtrées pour avoir les coordonnées (pour toutes, même rejetées, pour la carte complète)
        valid_geo = valid.merge(geo, on="cp", how="left")
        rejected_geo = rejected.merge(geo, on="cp", how="left")

        create_map(valid_geo, rejected_geo)
        send_file("carte_rene_costes.html")
    else:
        send_telegram("😴 René Costes - pas de nouvelles annonces répondant aux critères")

    print("✅ FIN")

if __name__ == "__main__":
    asyncio.run(main())
