# -*- coding: utf-8 -*-
"""
main.py — orchestrateur unique.

Remplace les 5 scripts indépendants :
  1. lance les 5 scrapers (un échec de site n'arrête pas les autres)
  2. dédoublonne TOUTES les annonces entre sites et avec l'historique
  3. applique les filtres métier (costes) puis le filtre CP
  4. génère UNE SEULE carte globale
  5. envoie UN SEUL message Telegram avec les vrais liens
  6. sauvegarde l'historique global unique
"""

import asyncio
import traceback
from datetime import datetime

import pandas as pd

from viager_common import (
    send_telegram, send_telegram_long, send_file,
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
        #   - entre sites (une même annonce sur 2 sites => 1 seule)
        #   - contre l'historique (jamais re-notifiée)
        connues = charge_historique()
        nouvelles, uniques = deduplique(df.to_dict("records"), connues)
        print(f"🧹 Doublons supprimés : {len(rows) - len(uniques)}")
        print(f"🆕 Nouvelles annonces : {len(nouvelles)}")

        if not nouvelles:
            send_telegram("😴 Aucune nouvelle annonce (5 sites)")
            return

        # ---------- 4) FILTRE CP (villes qui t'intéressent) ----------
        df_new = pd.DataFrame(nouvelles)
        avant = len(df_new)
        df_new = filtre_cp(df_new)
        print(f"📍 Filtre CP : {avant} → {len(df_new)} annonces")
        if len(df_new) == 0:
            send_telegram("😴 Nouvelles annonces, mais hors de ta zone")
            # On les enregistre quand même pour ne pas les revoir
            save_historique(nouvelles)
            return

        # ---------- 5) CARTE GLOBALE UNIQUE ----------
        df_new = geolocate(df_new)
        create_global_map(df_new)
        send_file("carte_globale.html")

        # ---------- 6) UN SEUL MESSAGE TELEGRAM ----------
        send_telegram_long(
            df_new.to_dict("records"),
            header=(f"🔥 {len(df_new)} nouvelle(s) annonce(s)\n"
                    f"({len(uniques)} uniques, doublons filtrés)"),
        )

        # ---------- 7) HISTORIQUE ----------
        save_historique(df_new.to_dict("records"))
        print("✅ FIN")

    except Exception as e:
        print(f"❌ ERREUR GLOBALE : {e}")
        traceback.print_exc()
        send_telegram(f"❌ ERREUR SCRAPER GLOBAL : {e}")


if __name__ == "__main__":
    asyncio.run(main())
