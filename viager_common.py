# -*- coding: utf-8 -*-
"""
viager_common.py
Base commune à tous les scrapers :
- envoi Telegram (message + fichier)
- nettoyage / extraction (prix, CP, type, titre)
- historique global UNIQUE (historique_global.csv)
- dédoublonnage inter-sites (titre flou + CP + prix)
- filtres métier (rente, bouquet, âge, villes autorisées)
- géolocalisation par code postal (UNE seule coordonnée par CP)
- carte Folium GLOBALE avec marqueurs standard + popups
"""

import os
import re
import unicodedata
from difflib import SequenceMatcher

import pandas as pd
import requests
import folium
from folium.features import DivIcon

# =========================================================
# CONFIG
# =========================================================

CSV_CP = "base-officielle-codes-postaux.csv"
HISTORY_FILE = "historique_global.csv"
OUTPUT_MAP = "carte_globale.html"

# ---- FILTRES METIER (viager René Costes) ----
RENTE_MAX = 1800
BOUQUET_MAX = 150000
FEMME_AGE_MIN = 90

# ---- FILTRE GEOGRAPHIQUE ----
CP_AUTORISES = set(
    os.getenv("CP_AUTORISES", "")
    .replace(",", " ")
    .split()
)

# Couleurs standard folium par site
SITE_COLORS = {
    "avoventes": "red",
    "costes": "blue",
    "encheres_immo": "green",
    "immonotaires": "orange",
    "vench": "purple",
}

# Icônes FontAwesome par type de bien (prefix fa)
TYPE_ICONS = {
    "Appartement": "building",
    "Maison": "home",
    "Villa": "home",
    "Terrain": "leaf",
    "Immeuble": "industry",
    "Local commercial": "store",
}


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram(message):
    """Envoie un message Telegram. Ne lève jamais d'exception."""
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": message},
            timeout=30,
        )
        return True
    except Exception as e:
        print(f"❌ TELEGRAM : {e}")
        return False


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
                timeout=60,
            )
    except Exception as e:
        print(f"❌ TELEGRAM FILE : {e}")


# =========================================================
# NETTOYAGE ET EXTRACTIONS COMMUNES
# =========================================================

def clean(txt):
    if txt is None:
        return ""
    txt = str(txt)
    for ch in ("\n", "\t", "\xa0", "\u202f"):
        txt = txt.replace(ch, " ")
    return re.sub(r"\s+", " ", txt).strip()


def detect_type(txt):
    t = clean(txt).lower()
    appart = ["appartement", "studio", "duplex", "triplex", "loft",
              "t1", "t2", "t3", "t4", "t5", "f1", "f2", "f3", "f4", "f5"]
    for k in appart:
        if k in t:
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


def extract_price(txt):
    """Plus petit montant plausible du bloc texte."""
    try:
        matches = re.findall(r"(\d[\d\s\u202f]{2,})\s?€", txt)
        vals = []
        for m in matches:
            m = re.sub(r"\s", "", m)
            if m.isdigit():
                v = int(m)
                if 1000 <= v <= 100_000_000:
                    vals.append(v)
        return min(vals) if vals else None
    except Exception:
        return None


def extract_cp(txt):
    try:
        m = re.findall(r"\b(\d{5})\b", txt)
        return m[0] if m else None
    except Exception:
        return None


def extract_surface(txt):
    try:
        m = re.search(r"(\d+(?:[\.,]\d+)?)\s?m²", txt, re.I)
        return m.group(1) if m else None
    except Exception:
        return None


# =========================================================
# NORMALISATION DE TITRE + DEDOUBLONNAGE
# =========================================================

STOPWORDS = {
    "le", "la", "les", "de", "du", "des", "a", "au", "aux", "et",
    "en", "sur", "d", "l", "pour", "avec", "vente", "encheres",
    "enchere", "viager", "immeuble", "maison", "appartement", "terrain",
    "villa", "m2", "pieces", "piece", "chambres", "chambre",
}


def normalise_titre(txt):
    """Titre normalisé : minuscules, sans accents, sans mots vides."""
    t = clean(txt).lower()
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    mots = [w for w in t.split() if w and w not in STOPWORDS]
    return " ".join(mots)


def titres_proches(a, b):
    if not a or not b:
        return False
    return SequenceMatcher(None, a, b).ratio() >= 0.65


def meme_annonce(row, autres):
    na = normalise_titre(row.get("titre") or row.get("txt", ""))
    for o in autres:
        if row.get("cp") and o.get("cp") and row["cp"] != o["cp"]:
            continue
        if row.get("type") != o.get("type"):
            continue
        no = normalise_titre(o.get("titre") or o.get("txt", ""))
        pa, pb = row.get("prix"), o.get("prix")
        prix_ok = (
            pa is None or pb is None
            or abs(pa - pb) <= max(300, 0.05 * max(pa, pb))
        )
        if prix_ok and (titres_proches(na, no) or (na and na == no)):
            return True
    return False


def deduplique(rows, connues=None):
    connues = connues or []
    nouvelles = []
    uniques = []
    for row in rows:
        if meme_annonce(row, uniques) or meme_annonce(row, connues):
            continue
        uniques.append(row)
        nouvelles.append(row)
    return nouvelles, uniques


# =========================================================
# HISTORIQUE GLOBAL
# =========================================================

def charge_historique():
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        df = pd.read_csv(HISTORY_FILE, sep=";")
    except Exception as e:
        print(f"⚠️ HISTORIQUE CORROMPU ({e}), on repart de zéro")
        return []
    if len(df) == 0:
        return []
    df = df.drop_duplicates(subset=["url"], keep="first")
    return df.to_dict("records")


def save_historique(rows):
    """Ajoute les nouvelles annonces à l'historique global unique."""
    anciennes = charge_historique()
    cols = ["site", "url", "titre", "type", "prix", "rente", "bouquet",
            "age", "cp", "date_vente", "surface", "pieces", "status"]
    df_old = pd.DataFrame(anciennes, columns=cols)
    df_new = pd.DataFrame(rows, columns=cols)
    combined = pd.concat([df_old, df_new], ignore_index=True)
    combined = combined.drop_duplicates(subset=["url"], keep="last")
    combined.to_csv(HISTORY_FILE, sep=";", index=False,
                    encoding="utf-8-sig")
    print(f"💾 HISTORIQUE : {len(combined)} annonces cumulées")


# =========================================================
# FILTRES
# =========================================================

def filtre_cp(df):
    """Ne garde que les CP autorisés (si la liste est renseignée)."""
    if not CP_AUTORISES:
        return df
    return df[df["cp"].astype(str).isin(CP_AUTORISES)]


def filtres_costes(df):
    """
    Filtres métier pour le viager René Costes uniquement :
    rente <= RENTE_MAX, bouquet <= BOUQUET_MAX,
    pas de femme < FEMME_AGE_MIN, pas de bien vendu.
    """
    df = df.copy()

    def rejet(row):
        txt = (row.get("txt") or "").lower()
        if "vendu" in txt:
            return True
        if row.get("rente") and row["rente"] > RENTE_MAX:
            return True
        if row.get("bouquet") and row["bouquet"] > BOUQUET_MAX:
            return True
        m = re.search(r"femme\s*,?\s*(\d{2})\s*ans", txt)
        if m and int(m.group(1)) < FEMME_AGE_MIN:
            return True
        return False

    masque = df.apply(rejet, axis=1)
    return df[~masque]


# =========================================================
# GEOLOCALISATION
# =========================================================

def geolocate(df):
    """
    Associe UNE coordonnée par annonce. La base officielle contient
    plusieurs lignes par code postal : on garde une seule par CP.
    """
    df = df.copy().drop_duplicates(subset=["url"])
    if len(df) == 0:
        df["lat"] = pd.NA
        df["lon"] = pd.NA
        return df
    geo = pd.read_csv(CSV_CP)
    geo = geo[["code_postal", "latitude", "longitude"]]
    geo.columns = ["cp", "lat", "lon"]
    geo["cp"] = geo["cp"].astype(str).str.strip()
    geo = geo.drop_duplicates(subset=["cp"], keep="first")
    df["cp"] = (df["cp"].fillna("").astype(str)
                .str.replace(".0", "", regex=False).str.strip())
    df = df.merge(geo, on="cp", how="left")
    print(f"📍 GEOLOCALISATION : {len(df)} annonces "
          f"({df['lat'].notna().sum()} géolocalisées)")
    return df


# =========================================================
# CARTE GLOBALE UNIQUE
# =========================================================
# Marqueurs standard folium.Icon (comme l'ancienne carte Costes)
# + popup construit comme dans le code d'origine.

def _popup(row):
    """Popup exactement dans le style du code d'origine."""
    lignes = [f"<b>{row.get('site')} — {row.get('type')}</b><br><br>"]
    if row.get("prix") is not None and not pd.isna(row.get("prix")):
        lignes.append(f"💰 Prix : {row['prix']} €<br>")
    if row.get("rente") is not None and not pd.isna(row.get("rente")):
        lignes.append(f"📆 Rente : {row['rente']} €/mois<br>")
    if row.get("bouquet") is not None and not pd.isna(row.get("bouquet")):
        lignes.append(f"💼 Bouquet : {row['bouquet']} €<br>")
    if row.get("age") is not None and not pd.isna(row.get("age")):
        lignes.append(f"👴 Age : {row['age']} ans<br>")
    if row.get("surface") is not None and not pd.isna(row.get("surface")):
        lignes.append(f"📐 Surface : {row['surface']} m²<br>")
    if row.get("date_vente"):
        lignes.append(f"📅 Vente : {row['date_vente']}<br>")
    if row.get("cp"):
        lignes.append(f"📍 CP : {row['cp']}<br><br>")
    if row.get("titre"):
        lignes.append(f"📝 {row['titre']}<br><br>")
    lignes.append(f'<a href="{row["url"]}" target="_blank">Voir annonce</a>')
    return "".join(lignes)


def create_global_map(df, output=OUTPUT_MAP):
    """Une seule carte pour tous les sites. UN point par annonce.
    Marqueurs folium.Icon standard : l'encart popup au clic
    fonctionne partout, comme dans l'ancien code."""
    df = df.copy().drop_duplicates(subset=["url"])
    m = folium.Map(location=[46.5, 2.5], zoom_start=6,
                   tiles="OpenStreetMap")

    for _, row in df.iterrows():
        try:
            if pd.isna(row.get("lat")):
                continue
            color = SITE_COLORS.get(row.get("site"), "gray")
            icon_name = TYPE_ICONS.get(row.get("type"), "home")
            folium.Marker(
                [row["lat"], row["lon"]],
                popup=folium.Popup(_popup(row), max_width=300),
                icon=folium.Icon(color=color, icon=icon_name, prefix="fa"),
            ).add_to(m)
        except Exception as e:
            print(f"❌ MARKER : {e}")

    m.save(output)
    print(f"✅ CARTE GLOBALE SAUVEGARDEE : {output} "
          f"({len(df[df['lat'].notna()])} points)")
