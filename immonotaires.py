# =========================================================
# IMMO NOTAIRES ENCHERES
# VERSION API JSON
# =========================================================

import pandas as pd
import requests
import folium
import traceback
import os
import re
import json

from folium.features import DivIcon


# =========================================================
# CONFIG
# =========================================================

API_URL = "https://immonotairesencheres.com/api/search"

CSV_CP = "base-officielle-codes-postaux.csv"

HISTORY_FILE = "historique_immonotaires.csv"

OUTPUT_MAP = "carte_immonotaires.html"

PAGE_SIZE = 100


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

        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": message
            },
            timeout=20
        )

    except Exception as e:

        print(e)


def send_file(path):

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        return

    try:

        with open(path, "rb") as f:

            requests.post(
                f"https://api.telegram.org/bot{token}/sendDocument",
                data={"chat_id": chat_id},
                files={"document": f},
                timeout=60
            )

    except Exception as e:

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

    txt = re.sub(r"\s+", " ", txt)

    return txt.strip()


# =========================================================
# TYPE
# =========================================================

def detect_type(src):

    try:

        return (
            src
            .get("referentiel_type_de_bien", {})
            .get("valeur", "Autre")
        )

    except:
        return "Autre"


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
# API SCRAPE
# =========================================================

def scrape():

    rows = []

    seen = set()

    start = 0

    while True:

        print("")
        print("=" * 60)
        print(f"📄 OFFSET {start}")
        print("=" * 60)

        payload = {

            "from": start,

            "size": PAGE_SIZE,

            "query": {

                "bool": {

                    "must": [

                        {
                            "term": {
                                "contentType": "bien"
                            }
                        },

                        {
                            "bool": {

                                "should": [

                                    {
                                        "range": {
                                            "montant": {
                                                "gte": 0
                                            }
                                        }
                                    },

                                    {
                                        "bool": {
                                            "must_not": [
                                                {
                                                    "exists": {
                                                        "field": "montant"
                                                    }
                                                }
                                            ]
                                        }
                                    }

                                ],

                                "minimum_should_match": 1
                            }
                        }

                    ]
                }
            },

            "sort": [

                {
                    "dateDebut": {
                        "order": "asc",
                        "missing": "_last"
                    }
                },

                {
                    "referenceBien.keyword": {
                        "order": "asc",
                        "missing": "_last"
                    }
                }

            ]
        }

        try:

            r = requests.post(
                API_URL,
                json=payload,
                timeout=60
            )

            print("🌐 STATUS =", r.status_code)

            data = r.json()

        except Exception as e:

            print("❌ API")
            print(e)

            break

        hits = data.get("hits", [])

        print("🧩 HITS =", len(hits))

        if len(hits) == 0:
            break

        added = 0

        for h in hits:

            try:

                src = h["_source"]

                doc_id = src.get("documentId")

                if not doc_id:
                    continue

                annonce_url = (
                    "https://immonotairesencheres.com/bien/"
                    + doc_id
                )

                if annonce_url in seen:
                    continue

                seen.add(annonce_url)

                titre = clean(
                    src.get("titre")
                )

                ville = clean(
                    src.get("ville")
                )

                cp = clean(
                    src.get("codePostal")
                )

                prix = (
                    src.get("montant")
                    or src.get("miseAPrixDefinitive")
                )

                surface = src.get("surface")

                pieces = src.get("nombrePiece")

                type_bien = detect_type(src)

                txt = " | ".join([
                    titre,
                    ville,
                    cp
                ])

                photo = None

                photos = src.get(
                    "listePhotos",
                    []
                )

                if len(photos) > 0:

                    photo = (
                        "https://immonotairesencheres.com"
                        + photos[0]["url"]
                    )

                row = {

                    "url": annonce_url,

                    "txt": txt,

                    "titre": titre,

                    "ville": ville,

                    "cp": cp,

                    "prix": prix,

                    "surface": surface,

                    "pieces": pieces,

                    "type": type_bien,

                    "photo": photo

                }

                rows.append(row)

                added += 1

                print(
                    f"✅ {type_bien} | "
                    f"{prix}€ | "
                    f"{cp}"
                )

            except Exception as e:

                print("❌ HIT")
                print(e)

        print("")
        print("➕ AJOUTES =", added)

        if added == 0:
            break

        start += PAGE_SIZE

    df = pd.DataFrame(rows)

    if len(df) > 0:

        df = df.drop_duplicates(
            subset="url"
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

            elif prix < 300000:

                color = "#8000ff"

            elif prix < 700000:

                color = "#00aa00"

            else:

                color = "#222222"

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
<b>{row['titre']}</b><br><br>

🏠 {row['type']}<br>

💰 {row['prix']} €<br>

📐 {row['surface']} m²<br>

🚪 {row['pieces']} pièces<br>

📍 {row['cp']} {row['ville']}<br><br>

<a href="{row['url']}" target="_blank">
Voir annonce
</a>
"""

            html = f"""
<div style="
width:42px;
display:flex;
justify-content:center;
align-items:center;
">

<div style="
background:{color};
width:38px;
height:38px;
border-radius:50%;
display:flex;
justify-content:center;
align-items:center;
font-size:16px;
border:2px solid white;
box-shadow:0 0 4px rgba(0,0,0,0.4);
">
{symbol}
</div>

</div>
"""

            folium.Marker(

                location=[
                    row["lat"],
                    row["lon"]
                ],

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

            ).add_to(m)

        except Exception as e:

            print(e)

    m.save(OUTPUT_MAP)

    print("✅ CARTE SAUVEGARDEE")


# =========================================================
# MAIN
# =========================================================

def main():

    try:

        print("")
        print("=" * 60)
        print("🚀 SCRAPING API")
        print("=" * 60)

        df = scrape()

        print("")
        print(f"📦 TOTAL = {len(df)}")

        if len(df) == 0:

            print("❌ AUCUNE ANNONCE")

            send_telegram(
                "❌ IMMO NOTAIRES\nAUCUNE ANNONCE"
            )

            return

        # =================================================
        # HISTORIQUE
        # =================================================

        try:

            old = pd.read_csv(
                HISTORY_FILE,
                sep=";"
            )

            old_urls = set(
                old["url"]
                .astype(str)
            )

            print("📚 HISTORIQUE =", len(old))

        except:

            print("⚠️ HISTORIQUE CORROMPU")

            old_urls = set()

        new_df = df[
            ~df["url"].isin(old_urls)
        ]

        print(
            "🆕 NOUVELLES =",
            len(new_df)
        )

        # =================================================
        # SAVE
        # =================================================

        df.to_csv(
            HISTORY_FILE,
            sep=";",
            index=False,
            encoding="utf-8-sig"
        )

        print("💾 HISTORIQUE OK")

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

        send_file(OUTPUT_MAP)

        send_telegram(
            f"✅ IMMO NOTAIRES\n"
            f"📦 TOTAL = {len(df)}\n"
            f"🆕 NOUVELLES = {len(new_df)}"
        )

        print("✅ FIN")

    except Exception as e:

        print("")
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

    main()
