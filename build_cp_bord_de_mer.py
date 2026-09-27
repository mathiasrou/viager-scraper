# -*- coding: utf-8 -*-
"""
build_cp_bord_de_mer.py

Construit cp_bord_de_mer.csv : la base des codes postaux des communes
littorales françaises (loi littoral, classements "Mer" et "Estuaire").

Sources :
- Liste officielle des communes de la loi littoral (932 communes
  classées "Mer"), copie CSV publique (mise à jour 2019).
- base-officielle-codes-postaux.csv (dans le dépôt) : correspondance
  code INSEE -> code postal + coordonnées GPS.

Sortie : cp_bord_de_mer.csv (séparateur ";", encodage utf-8-sig)
Colonnes : code_postal;commune;departement;code_insee;classement;population;latitude;longitude
"""

import io

import pandas as pd
import requests

URL_LITTORAL = (
    "https://raw.githubusercontent.com/maelvdev/R605_Graphique_Immobilier"
    "/main/files/communes_littorales_2019.csv"
)
CSV_CP = "base-officielle-codes-postaux.csv"
OUT = "cp_bord_de_mer.csv"


def charge_littoral():
    r = requests.get(URL_LITTORAL, timeout=60)
    r.raise_for_status()
    try:
        txt = r.content.decode("utf-8")
    except UnicodeDecodeError:
        txt = r.content.decode("latin-1")
    return pd.read_csv(
        io.StringIO(txt),
        sep=";",
        header=None,
        names=["region", "dept", "departement", "code_insee", "commune",
               "classement", "motif", "entite", "population", "chef_lieu"],
        dtype=str,
    )


def main():
    litt = charge_littoral()
    litt["code_insee"] = litt["code_insee"].astype(str).str.strip()
    print(f"🌊 Communes loi littoral : {len(litt)}")
    print(f"   Classements : {litt['classement'].value_counts().to_dict()}")

    geo = pd.read_csv(CSV_CP, dtype=str)
    geo = geo[["code_commune_insee", "code_postal", "latitude", "longitude"]]
    geo = geo.rename(columns={"code_commune_insee": "code_insee"})
    geo["code_insee"] = geo["code_insee"].astype(str).str.strip()
    geo["code_postal"] = geo["code_postal"].astype(str).str.strip()

    df = litt.merge(geo, on="code_insee", how="left")
    sans_cp = df[df["code_postal"].isna()]
    print(f"   Sans correspondance CP : {len(sans_cp)} communes "
          f"(fusions postérieures à 2019, ignorées)")
    df = df[df["code_postal"].notna()]
    df = df.drop_duplicates(subset=["code_postal", "code_insee"])
    df["population"] = pd.to_numeric(df["population"], errors="coerce")

    out = df[["code_postal", "commune", "departement", "code_insee",
              "classement", "population", "latitude", "longitude"]]
    out = out.sort_values(["code_postal", "commune"])
    out.to_csv(OUT, sep=";", index=False, encoding="utf-8-sig")

    nb_cp = out["code_postal"].nunique()
    print(f"✅ {OUT} : {len(out)} lignes, {nb_cp} codes postaux, "
          f"{out['code_insee'].nunique()} communes")
    top = out.groupby("departement")["code_postal"].nunique()
    top = top.sort_values(ascending=False)
    for dep, n in top.head(10).items():
        print(f"     {dep} : {n} CP")


if __name__ == "__main__":
    main()
