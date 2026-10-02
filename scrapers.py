# -*- coding: utf-8 -*-
"""
scrapers.py
Un scraper par site. Chaque fonction renvoie une LISTE de dictionnaires
au schéma commun :
  site, url, titre, type, prix, rente, bouquet, age, cp,
  date_vente, surface, pieces, status, txt
"""

import re
import asyncio

import requests
from playwright.async_api import async_playwright

from viager_common import (
    clean, detect_type, extract_price, extract_cp,
    extract_surface,
)

MOIS = {
    "janvier": "01", "février": "02", "mars": "03", "avril": "04",
    "mai": "05", "juin": "06", "juillet": "07", "août": "08",
    "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
}


def _row(site, **kwargs):
    row = {
        "site": site, "url": None, "titre": None, "type": "Autre",
        "prix": None, "rente": None, "bouquet": None, "age": None,
        "cp": None, "date_vente": None, "surface": None, "pieces": None,
        "status": "inconnu", "txt": "",
    }
    row.update(kwargs)
    return row


def _to_int(s):
    """"36 000" / "36.000" / "36 000,00" -> 36000."""
    s = re.sub(r"[\s\u202f\xa0]", "", str(s))
    m = re.match(r"^(\d+)[.,]\d{1,2}$", s)
    if m:
        s = m.group(1)
    else:
        s = re.sub(r"[.,](?=\d{3}\b)", "", s)
    return int(s) if s.isdigit() else None


def prix_euros(txt):
    """
    Extraction de prix robuste : "36 000 €", "36.000 €", "36 000,00 €",
    "36 000 EUR", avec libellés "Mise à prix", "Prix de départ",
    "Prix initial", "Valeur estimée".
    """
    if not txt:
        return None
    labellise = re.compile(
        r"(?:mise\s*[aà]\s*prix|prix\s*de\s*d[eé]part|prix\s*initial"
        r"|valeur\s*estim[eé]e|prix\s*de\s*r[eé]servation|prix)"
        r"[^\d]{0,20}([\d][\d\s.,\u202f\xa0]{2,})\s*(?:€|EUR|euros?)",
        re.I,
    )
    for m in labellise.finditer(txt):
        v = _to_int(m.group(1))
        if v and 100 <= v <= 100_000_000:
            return v
    vals = []
    for m in re.finditer(r"([\d][\d\s.,\u202f\xa0]{2,})\s*(?:€|EUR|euros?)", txt, re.I):
        v = _to_int(m.group(1))
        if v and 100 <= v <= 100_000_000:
            vals.append(v)
    return min(vals) if vals else None


# =========================================================
# 1) AVOVENTES
# =========================================================
# Fix : seuls les liens du domaine avoventes.fr sont retenus
# (avant, le lien de la bannière cookies cookiebot.com était
# associé aux annonces => mauvais "Voir annonce").

async def scrape_avoventes():
    BASE_URL = "https://avoventes.fr/recherche/toutes"
    rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        page = await browser.new_page(
            viewport={"width": 1600, "height": 4000}
        )
        await page.goto(BASE_URL, wait_until="networkidle", timeout=120000)

        try:
            await page.locator("button:has-text('Tout accepter')").click(timeout=5000)
            await page.wait_for_timeout(3000)
        except Exception:
            pass

        cards = await page.evaluate(
            """
            () => {
              const out = [];
              const bad = /(cookiebot|javascript:|mailto:|^#|utm_source)/i;
              for (const a of document.querySelectorAll('a[href]')) {
                try {
                  const u = new URL(a.href);
                  if (!/avoventes\\.fr$/.test(u.hostname)) continue;
                  if (bad.test(a.href)) continue;
                } catch (e) { continue; }
                let c = a.closest('article, li, div');
                for (let i = 0; i < 6 && c; i++) {
                  const t = c.innerText || '';
                  if (/Mise \u00e0 prix/i.test(t)) {
                    out.push({ href: a.href, txt: t });
                    break;
                  }
                  c = c.parentElement;
                }
              }
              return out;
            }
            """
        )
        print(f"🧩 AVOVENTES : {len(cards)} cartes avec lien")

        if not cards:
            body = clean(await page.locator("body").inner_text())
            blocs = re.split(r"Vente aux enchères", body)
            cards = [
                {"href": BASE_URL, "txt": b}
                for b in blocs if len(clean(b)) >= 100
            ]
            print("⚠️ AVOVENTES : pas de liens trouvés, mode texte seul")

        seen = set()
        for card in cards:
            try:
                url = card["href"]
                txt = clean(card["txt"])
                if url in seen or len(txt) < 100:
                    continue
                seen.add(url)

                prix = prix_euros(txt) or extract_price(txt)
                cp = extract_cp(txt)
                titre = clean(txt.split("Mise à prix")[0])[-150:]

                date_vente = None
                m = re.search(
                    r"Date de la vente\s*:\s*(.+?)(?:Date des visites|$)",
                    txt, re.S | re.I)
                if m:
                    d = re.search(r"(\d{2})\s+(\w+)\s+(\d{4})",
                                  clean(m.group(1)), re.I)
                    if d and d.group(2).lower() in MOIS:
                        date_vente = (f"{d.group(1)}/"
                                      f"{MOIS[d.group(2).lower()]}/"
                                      f"{d.group(3)}")

                t = txt.lower()
                if "adjugé" in t:
                    status = "terminee"
                elif "retirée" in t:
                    status = "retiree"
                elif "reportée" in t:
                    status = "reportee"
                else:
                    status = "future"

                rows.append(_row(
                    "avoventes", url=url, titre=titre, txt=txt,
                    type=detect_type(txt), prix=prix, cp=cp,
                    date_vente=date_vente, status=status,
                ))
            except Exception as e:
                print(f"❌ AVOVENTES carte : {e}")

        await browser.close()

    print(f"✅ AVOVENTES : {len(rows)} annonces")
    return rows


# =========================================================
# 2) COSTES VIAGER
# =========================================================

async def scrape_costes(max_annonces=100):
    URL = "https://www.costes-viager.com/acheter/annonces"
    DOMAINE = "https://www.costes-viager.com"
    rows = []
    seen = set()

    def money(label, txt):
        m = re.search(label + r".*?([\d\s]+)\s?€", txt, re.I)
        if not m:
            return None
        digits = re.sub(r"[^\d]", "", m.group(1))
        return int(digits) if digits else None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(URL, timeout=60000)

        try:
            btn = await page.wait_for_selector(
                "button:has-text('Accepter')", timeout=5000)
            await btn.click()
        except Exception:
            pass

        await page.wait_for_selector("rc-card-annonce")
        total = 0

        while True:
            cards = await page.query_selector_all("rc-card-annonce")
            for card in cards:
                if total >= max_annonces:
                    break
                try:
                    a = await card.query_selector("a")
                    href = await a.get_attribute("href") if a else None
                    url = DOMAINE + href if href else None
                    if not url or url in seen:
                        continue
                    seen.add(url)

                    txt = clean(await card.inner_text())
                    ages = [int(x) for x in
                            re.findall(r"(\d{2})\s*ans", txt, re.I)]

                    rows.append(_row(
                        "costes", url=url, txt=txt,
                        titre=clean(txt.split("\n")[0])[:150],
                        type=detect_type(txt),
                        bouquet=money("Bouquet", txt),
                        rente=money("Rente", txt),
                        age=max(ages) if ages else None,
                        cp=extract_cp(txt),
                    ))
                    total += 1
                except Exception as e:
                    print(f"❌ COSTES carte : {e}")

            if total >= max_annonces:
                break

            btn = await page.query_selector("button:has-text('Afficher plus')")
            if not btn:
                break
            old_count = len(cards)
            await btn.click()
            try:
                await page.wait_for_function(
                    "(old) => document.querySelectorAll('rc-card-annonce')"
                    ".length > old",
                    arg=old_count, timeout=10000,
                )
            except Exception:
                break

        await browser.close()

    print(f"✅ COSTES : {len(rows)} annonces")
    return rows


# =========================================================
# 3) ENCHERES IMMO
# =========================================================

async def scrape_encheres_immo():
    BASE_URL = "https://encheres-immo.com/annonces"
    DOMAINE = "https://encheres-immo.com"
    rows = []
    seen = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(BASE_URL, timeout=60000)
        await page.wait_for_timeout(5000)

        while True:
            articles = await page.query_selector_all("article")
            if not articles:
                break

            for article in articles:
                try:
                    txt = clean(await article.inner_text())
                    if len(txt) < 20:
                        continue

                    url = None
                    for link in await article.query_selector_all("a"):
                        href = await link.get_attribute("href")
                        if href:
                            url = (DOMAINE + href
                                   if href.startswith("/") else href)
                            break
                    if not url or url in seen:
                        continue
                    seen.add(url)

                    m = re.search(
                        r"(Débute|Termine)\s+le\s+(\d{2}/\d{2}/\d{4})",
                        txt, re.I)
                    date_vente = m.group(2) if m else None

                    t = txt.lower()
                    if "vente terminée" in t:
                        status = "terminee"
                    elif "termine le" in t:
                        status = "en_cours"
                    elif "débute le" in t:
                        status = "future"
                    else:
                        status = "inconnu"

                    m_pieces = re.search(r"(\d+)\s*pi[eè]ces", txt, re.I)
                    m_surf = re.search(r"(\d+)\s?m²", txt, re.I)

                    rows.append(_row(
                        "encheres_immo", url=url, txt=txt,
                        prix=prix_euros(txt) or extract_price(txt),
                        cp=extract_cp(txt), type=detect_type(txt),
                        surface=m_surf.group(1) if m_surf else None,
                        pieces=m_pieces.group(1) if m_pieces else None,
                        date_vente=date_vente, status=status,
                    ))
                except Exception as e:
                    print(f"❌ ENCHERES IMMO article : {e}")

            try:
                next_btn = page.locator("text=Suivant")
                if await next_btn.count() == 0:
                    break
                await next_btn.first.click()
                await page.wait_for_timeout(5000)
            except Exception:
                break

        await browser.close()

    print(f"✅ ENCHERES IMMO : {len(rows)} annonces")
    return rows


# =========================================================
# 4) IMMONOTAIRES  (API JSON, sans navigateur)
# =========================================================

def scrape_immonotaires():
    API_URL = "https://immonotairesencheres.com/api/search"
    PAGE_SIZE = 100
    rows = []
    seen = set()
    start = 0

    while True:
        payload = {
            "from": start, "size": PAGE_SIZE,
            "query": {"bool": {"must": [
                {"term": {"contentType": "bien"}},
                {"bool": {"should": [
                    {"range": {"montant": {"gte": 0}}},
                    {"bool": {"must_not": [{"exists": {"field": "montant"}}]}},
                ], "minimum_should_match": 1}},
            ]}},
            "sort": [
                {"dateDebut": {"order": "asc", "missing": "_last"}},
                {"referenceBien.keyword": {"order": "asc", "missing": "_last"}},
            ],
        }
        try:
            r = requests.post(API_URL, json=payload, timeout=60)
            hits = r.json().get("hits", [])
        except Exception as e:
            print(f"❌ IMMONOTAIRES API : {e}")
            break
        if not hits:
            break

        added = 0
        for h in hits:
            try:
                src = h["_source"]
                doc_id = src.get("documentId")
                if not doc_id:
                    continue
                url = f"https://immonotairesencheres.com/bien/{doc_id}"
                if url in seen:
                    continue
                seen.add(url)

                titre = clean(src.get("titre"))
                cp = clean(src.get("codePostal"))

                rows.append(_row(
                    "immonotaires", url=url, titre=titre, txt=titre,
                    type=clean((src.get("referentiel_type_de_bien") or {})
                               .get("valeur", "Autre")),
                    prix=src.get("montant") or src.get("miseAPrixDefinitive"),
                    surface=src.get("surface"),
                    pieces=src.get("nombrePiece"),
                    cp=cp,
                ))
                added += 1
            except Exception as e:
                print(f"❌ IMMONOTAIRES hit : {e}")

        if added == 0:
            break
        start += PAGE_SIZE

    print(f"✅ IMMONOTAIRES : {len(rows)} annonces")
    return rows


# =========================================================
# 5) VENCH  (pages de détail)
# =========================================================

async def scrape_vench(max_pages=5):
    BASE_URL = "https://www.vench.fr/prochaines-ventes-aux-encheres.html"
    rows = []
    seen = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            viewport={"width": 1600, "height": 4000})
        page = await context.new_page()

        for page_num in range(1, max_pages + 1):
            try:
                url = BASE_URL if page_num == 1 else f"{BASE_URL}?p={page_num}"
                await page.goto(url, wait_until="domcontentloaded",
                                timeout=120000)
                await page.wait_for_timeout(5000)

                links = list(set(await page.eval_on_selector_all(
                    "a",
                    "els => els.map(e => e.href)"
                    ".filter(h => h.includes('/vente-'))",
                )))
                print(f"📄 VENCH page {page_num} : {len(links)} liens")

                for detail_url in links:
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)
                    detail = None
                    try:
                        detail = await context.new_page()
                        await detail.goto(detail_url,
                                          wait_until="domcontentloaded",
                                          timeout=120000)
                        # laisser le prix (chargé en JS) s'afficher :
                        # scroll + attente plus longue
                        await detail.evaluate(
                            "window.scrollTo(0, document.body.scrollHeight)")
                        await detail.wait_for_timeout(6000)

                        title = await detail.title()
                        txt = clean(await detail.locator("body").inner_text())
                        full = title + "\n" + txt

                        try:
                            h1 = clean(await detail.locator("h1").inner_text())
                        except Exception:
                            h1 = title

                        m = re.search(r"(\d{2}/\d{2}/\d{4})", full)

                        t = full.lower()
                        if "adjugé" in t:
                            status = "terminee"
                        elif "retirée" in t:
                            status = "retiree"
                        elif "reportée" in t:
                            status = "reportee"
                        else:
                            status = "future"

                        ages = [int(x) for x in
                                re.findall(r"(\d{2})\s*ans", full, re.I)]

                        prix = prix_euros(full) or extract_price(full)

                        rows.append(_row(
                            "vench", url=detail_url, txt=full[:500],
                            titre=h1[:150], type=detect_type(full),
                            prix=prix, cp=extract_cp(full),
                            date_vente=m.group(1) if m else None,
                            surface=extract_surface(full),
                            age=max(ages) if ages else None,
                            status=status,
                        ))
                    except Exception as e:
                        print(f"❌ VENCH détail : {e}")
                    finally:
                        if detail:
                            await detail.close()

            except Exception as e:
                print(f"❌ VENCH page {page_num} : {e}")

        await browser.close()

    print(f"✅ VENCH : {len(rows)} annonces")
    return rows
