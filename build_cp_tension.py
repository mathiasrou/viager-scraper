# -*- coding: utf-8 -*-
"""
build_cp_tension.py

Construit cp_tension_locative.csv : les codes postaux des villes
françaises agréables à vivre ET en forte tension locative
(toute la France, pas seulement le littoral).

Sources des classements croisés (2025-2026) :
- Baromètre de la tension locative Manda S1 2026 (candidats/annonce,
  score de tension) : Nice, Paris, Marseille, Lyon, Bordeaux,
  Montpellier, Toulouse...
- Tensiomètre LocService (ratio demande/offre) : Lyon, Rennes, Paris,
  Caen, Annecy, Bordeaux, Angers...
- Palmarès "Villes où il fait bon vivre" (cadre de vie) : Annecy,
  La Rochelle, Bayonne, Arcachon, Antibes...

La liste VILLES ci-dessous est ÉDITABLE : ajoute ou retire
librement des noms de communes (casse et accents indifférents).

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
    # Grandes métropoles en forte tension (baromètre Manda 2026)
    "Paris", "Lyon", "Villeurbanne", "Bordeaux", "Marseille",
    "Montpellier", "Toulouse", "Nice", "Rennes", "Nantes",
    "Strasbourg", "Grenoble",
    # Villes moyennes agréables ET tendues (cadre de vie + demande)
    "Annecy", "Angers", "Caen", "La Rochelle", "Bayonne",
    "Arcachon", "Aix-en-Provence", "Antibes", "Menton",
    "Annemasse", "Thonon-les-Bains", "Lille", "Le Havre",
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
    for ville, n in out.groupby("commune")["code_postal"].nunique()\
            .sort_values(ascending=False).items():
        print(f"     {ville} : {n} CP")


if __name__ == "__main__":
    main()
