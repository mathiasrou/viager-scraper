# -*- coding: utf-8 -*-
"""
main.py — orchestrateur unique.

  1. lance les 5 scrapers (un échec de site n'arrête pas les autres)
  2. dédoublonne TOUTES les annonces entre sites et avec l'historique
  3. applique les filtres métier (costes) puis le filtre CP
  4. sauvegarde l'historique (une ligne / annonce)
  5. génère UNE SEULE carte globale (tuiles OSM, un point par annonce)
     avec les NOUVELLES annonces + celles des 7 derniers jours
  6. envoie la carte en FICHIER Telegram (sendDocument) :
     méthode d'origine dont les popups fonctionnaient dans Telegram
"""

import asyncio
import os
import traceback
from datetime import datetime

import pandas as pd

from viager_common import (
    send_telegram, send_file,
    save_historique, charge_historique, deduplique,
    filtre_cp, filtres_costes, geolocate, create_global_map,
    historique_recent, OUTPUT_MAP,
)
import scrapers


async def run_all_scrapers():
    """Lance les 5 scrapers, isole les erreurs par site."""

    async def safe_async(nom, coro):
        try:
            return await asyncio.wait_for(coro, timeout=600)
        except Exception as e:
            print(f"❌ {nom} : {e}")
            traceback.print_exc()
            send_telegram(f"⚠️ Scraper {nom} en échec : {e}")
            return []

    def safe_sync(nom, fn):
        try:
            return fn()
        except Exception as e:
            print(f"❌ {nom} : {e}")
            traceback.print_exc()
            send_telegram(f"⚠️ Scraper {nom} en échec : {e}")
            return []

    costes = await safe_async("costes", scrapers.scrape_costes())
    avoventes = await safe_async("avoventes", scrapers.scrape_avoventes())
    encheres = await safe_async("encheres_immo", scrapers.scrape_encheres_immo())
    vench = await safe_async("vench", scrapers.scrape_vench())
    immonotaires = safe_sync("immonotaires", scrapers.scrape_immonotaires)

    return avoventes + costes + encheres + immonotaires + vench


async def main():
    print("🚀 DÉMARRAGE —", datetime.now().strftime("%d/%m/%Y %H:%M"))
    try:
        # ---------- 1) SCRAPING ----------
        rows = await run_all_scrapers()
        print(f"📦 TOTAL SCRAPPÉ : {len(rows)} annonces")

        if not rows:
            send_telegram("❌ Aucune annonce scrapée sur les 5 sites")
            return

        # ---------- 2) FILTRES MÉTIER (costes uniquement) ----------
        df = pd.DataFrame(rows)
        df_costes = df[df["site"] == "costes"]
        df_autres = df[df["site"] != "costes"]
        df_costes_ok = filtres_costes(df_costes)
        df = pd.concat([df_costes_ok, df_autres], ignore_index=True)
        print(f"✅ Après filtres métier : {len(df)} annonces")

        # ---------- 3) DÉDOUBLONNAGE GLOBAL ----------
        connues = charge_historique()
        nouvelles, uniques = deduplique(df.to_dict("records"), connues)
        print(f"🧹 Doublons supprimés : {len(rows) - len(uniques)}")
        print(f"🆕 Nouvelles annonces : {len(nouvelles)}")

        save_historique(nouvelles)

        # ---------- 4) CARTE : nouvelles (filtrées CP) + 7 derniers jours ----------
        df_carte = pd.DataFrame()
        nb_nouvelles_zone = 0
        if nouvelles:
            df_new = filtre_cp(pd.DataFrame(nouvelles))
            nb_nouvelles_zone = len(df_new)
            print(f"📍 Filtre CP (nouvelles) : {len(nouvelles)} → {nb_nouvelles_zone}")
            if nb_nouvelles_zone:
                df_carte = pd.concat([df_carte, df_new], ignore_index=True)

        df_hist = pd.DataFrame(historique_recent(7))
        if len(df_hist):
            df_hist = filtre_cp(df_hist)
            print(f"📍 Historique 7j dans la zone : {len(df_hist)} annonces")
            if len(df_hist):
                df_carte = pd.concat([df_carte, df_hist], ignore_index=True)

        if len(df_carte) and "url" in df_carte.columns:
            df_carte = df_carte.drop_duplicates(subset=["url"])

        carte_avant = None
        if os.path.exists(OUTPUT_MAP):
            try:
                carte_avant = open(OUTPUT_MAP, encoding="utf-8").read()
            except Exception:
                carte_avant = None

        # ---------- 5) CARTE GLOBALE UNIQUE ----------
        df_carte = geolocate(df_carte)
        create_global_map(df_carte)
        nb_points = (int(df_carte["lat"].notna().sum())
                     if "lat" in df_carte.columns else 0)

        try:
            carte_apres = open(OUTPUT_MAP, encoding="utf-8").read()
        except Exception:
            carte_apres = None

        # ---------- 6) TELEGRAM : la carte en FICHIER (méthode d'origine) ----------
        if nb_nouvelles_zone:
            par_site = df_new["site"].value_counts().to_dict()
            detail = "\n".join(f"• {s} : {n}" for s, n in par_site.items())
            send_telegram(
                f"🗺️ {nb_nouvelles_zone} nouvelle(s) annonce(s)\n"
                f"{detail}\n"
                f"📍 Carte : {nb_points} points (nouvelles + 7 derniers jours)"
            )
            send_file("carte_globale.html")
        elif nouvelles:
            send_telegram("😴 Nouvelles annonces, mais hors de ta zone")
        elif nb_points and carte_avant != carte_apres:
            send_telegram(
                f"🗺️ Carte mise à jour : {nb_points} annonces visibles "
                f"(7 derniers jours)"
            )
            send_file("carte_globale.html")
        else:
            send_telegram("😴 Aucune nouvelle annonce (5 sites)")

        print("✅ FIN")

    except Exception as e:
        print(f"❌ ERREUR GLOBALE : {e}")
        traceback.print_exc()
        send_telegram(f"❌ ERREUR SCRAPER GLOBAL : {e}")


if __name__ == "__main__":
    asyncio.run(main())
