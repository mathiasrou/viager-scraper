# -*- coding: utf-8 -*-
"""
main.py — orchestrateur unique.

  1. lance les 5 scrapers (un échec de site n'arrête pas les autres)
  2. dédoublonne TOUTES les annonces entre sites et avec l'historique
  3. applique les filtres métier (costes) puis le filtre CP
  4. sauvegarde l'historique (une ligne / annonce)
  5. génère UNE SEULE carte globale (tuiles OSM, un point par annonce)
  6. envoie la carte en FICHIER Telegram (sendDocument) :
     méthode d'origine dont les popups fonctionnaient dans Telegram
"""

import asyncio
import traceback
from datetime import datetime

import pandas as pd

from viager_common import (
    send_telegram, send_file,
    save_historique, charge_historique, deduplique,
    filtre_cp, filtres_costes, geolocate, create_global_map,
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

        if not nouvelles:
            send_telegram("😴 Aucune nouvelle annonce (5 sites)")
            return

        # ---------- 4) FILTRE CP ----------
        df_new = pd.DataFrame(nouvelles)
        avant = len(df_new)
        df_new = filtre_cp(df_new)
        print(f"📍 Filtre CP : {avant} → {len(df_new)} annonces")
        if len(df_new) == 0:
            send_telegram("😴 Nouvelles annonces, mais hors de ta zone")
            return

        # ---------- 5) CARTE GLOBALE UNIQUE ----------
        df_new = geolocate(df_new)
        create_global_map(df_new)

        # ---------- 6) TELEGRAM : la carte en FICHIER (méthode d'origine) ----------
        par_site = df_new["site"].value_counts().to_dict()
        detail = "\n".join(f"• {s} : {n}" for s, n in par_site.items())
        send_telegram(
            f"🗺️ {len(df_new)} nouvelle(s) annonce(s)\n"
            f"{detail}"
        )
        send_file("carte_globale.html")

        print("✅ FIN")

    except Exception as e:
        print(f"❌ ERREUR GLOBALE : {e}")
        traceback.print_exc()
        send_telegram(f"❌ ERREUR SCRAPER GLOBAL : {e}")


if __name__ == "__main__":
    asyncio.run(main())
