# -*- coding: utf-8 -*-
"""
build_cp_tension.py

Construit cp_tension_locative.csv : les codes postaux des villes
françaises agréables à vivre ET en forte tension locative
(toute la France, pas seulement le littoral).

Sources croisées (2025-2026) :
- Baromètre Manda S1 2026 (candidats/annonce, score de tension)
- Tensiomètre LocService (ratio demande/offre)
- Palmarès "Villes et villages où il fait bon vivre" (cadre de vie)

La liste VILLES est ÉDITABLE : ajoute ou retire librement des noms
de communes (casse et accents indifférents).

Sortie : cp_tension_locative.csv (;, utf-8-sig)
Colonnes : code_postal;commune
"""

import re
import unicodedata

import pandas as pd

CSV_CP = "base-officielle-codes-postaux.csv"
OUT = "cp_tension_locative.csv"

# Villes agréables à vivre + forte tension locative (France entière).
VILLES = [
    # --- Métropoles en très forte tension (Manda S1 2026, LocService) ---
    "Paris", "Boulogne-Billancourt", "Neuilly-sur-Seine", "Saint-Cloud",
    "Vincennes", "Saint-Mandé",
    "Lyon", "Villeurbanne", "Bordeaux", "Marseille", "Montpellier",
    "Toulouse", "Nice", "Rennes", "Nantes", "Strasbourg", "Lille",
    "Grenoble",
    # --- Villes agréables à vivre ET tendues (France entière) ---
    "Annecy", "Annemasse", "Thonon-les-Bains", "Aix-en-Provence",
    "Angers", "Caen", "La Rochelle", "Bayonne", "Anglet", "Biarritz",
    "Arcachon", "La Teste-de-Buch", "Lège-Cap-Ferret", "Le Havre",
    # --- Villes agréables de l'intérieur, forte demande locative ---
    "Tours", "Amboise", "Versailles", "Charenton-le-Pont",
    "Saint-Germain-en-Laye", "Fontainebleau", "Chartres",
    "Poitiers", "Orléans", "Dijon", "Metz", "Avignon",
    "Colmar", "Chambéry", "Reims",
    # --- Littoral attractif à forte demande (Manche/Atlantique) ---
    "Deauville", "Honfleur", "Le Touquet-Paris-Plage", "Dinard",
    "Saint-Malo", "Vannes", "Quiberon", "Carnac",
    "La Baule-Escoublac", "Le Croisic", "Les Sables-d'Olonne",
    "Pornic", "Royan",
    "Saint-Jean-de-Luz", "Hendaye", "Guéthary", "Sète",
    # --- Île de Ré (très tendu, très prisé) ---
    "Saint-Martin-de-Ré", "Sainte-Marie-de-Ré", "La Flotte",
    "Rivedoux-Plage", "La Couarde-sur-Mer", "Les Portes-en-Ré",
    "Ars-en-Ré", "Loix",
    # --- Littoral méditerranéen tendu ---
    "Antibes", "Cannes", "Menton", "Cassis", "Saint-Raphaël",
    "Fréjus", "Hyères", "Six-Fours-les-Plages", "Sanary-sur-Mer",
    "Bandol", "Collioure",
]


def normalise(nom):
    t = str(nom).upper()
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = re.sub(r"[^A-Z0-9]", "", t)
    return t


def main():
    geo = pd.read_csv(CSV_CP, dtype=str)
    geo["cle"] = geo["nom_de_la_commune"].map(normalise)
    cibles = {normalise(v): v for v in VILLES}

    df = geo[geo["cle"].isin(cibles)].copy()
    df["commune"] = df["cle"].map(cibles)
    manquantes = [v for k, v in cibles.items() if k not in set(df["cle"])]
    if manquantes:
        print(f"⚠️ Communes non trouvées dans la base CP : {manquantes}")

    out = df[["code_postal", "commune"]].drop_duplicates()
    out = out.sort_values(["commune", "code_postal"])
    out.to_csv(OUT, sep=";", index=False, encoding="utf-8-sig")

    print(f"✅ {OUT} : {len(out)} CP, "
          f"{out['commune'].nunique()} villes")


if __name__ == "__main__":
    main()
