"""
hosting_parser.py — поиск хостинг-компаний и ЦОД с расходами на электроэнергию > 10 млн руб/мес

Алгоритм:
  1. Поиск компаний по ОКВЭД через egrul.nalog.ru (бесплатно, без ключей)
  2. Получение финансовых отчётов через ГИР БО (bo.nalog.ru)
  3. Фильтр: прокси расходов на электроэнергию > 10 млн руб/месяц
     Прокси = себестоимость × 0.4 / 12  (электроэнергия ≈ 40% себестоимости хостинга)

Установка:
    pip install requests pandas tqdm
"""

import os
import re
import time
import random
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import date
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────────────────────

# ОКВЭД хостинга / ЦОД
HOSTING_OKVEDS = [
    "63.11",   # Обработка данных, хостинг и сопутствующие услуги
    "62.09",   # Прочая деятельность в области ИТ
    "62.01",   # Разработка компьютерного ПО
    "35.11",   # Производство электроэнергии (в паре с другими ОКВЭД — может быть майнинг)
]

# Регионы: числовой код ЕГРЮЛ → название
REGIONS = {
    "38": "Иркутская область",
    "24": "Красноярский край",
    "19": "Республика Хакасия",
    "03": "Республика Бурятия",
    "10": "Республика Карелия",
    "07": "Кабардино-Балкарская Республика",
    "05": "Республика Дагестан",
}

# Порог: 10 млн руб/месяц на электроэнергию
ELECTRICITY_MONTHLY_THRESHOLD = 10_000_000
# Прокси: если нет данных себестоимости, используем выручку
REVENUE_PROXY_THRESHOLD = 300_000_000  # 300 млн/год ≈ электро > 10 млн/мес

MAX_PAGES_PER_QUERY = 10   # Страниц ЕГРЮЛ на один запрос

# ─────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]


def _ua() -> str:
    return random.choice(USER_AGENTS)


def _sleep(lo=1.5, hi=3.5):
    time.sleep(random.uniform(lo, hi))


# ─────────────────────────────────────────────────────────
# ЕГРЮЛ (egrul.nalog.ru)
# ─────────────────────────────────────────────────────────

def _make_egrul_session() -> requests.Session:
    """Инициализирует сессию с egrul.nalog.ru (получает cookies)."""
    session = requests.Session()
    try:
        session.get(
            "https://egrul.nalog.ru/",
            headers={
                "User-Agent": _ua(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            },
            timeout=15,
        )
        _sleep(1.0, 2.0)
        log.info("Сессия egrul.nalog.ru инициализирована.")
    except Exception as e:
        log.warning(f"Не удалось инициализировать сессию ЕГРЮЛ: {e}")
    return session


def _egrul_search_token(session: requests.Session, region: str, okvd: str) -> str | None:
    """
    POST /  → возвращает токен поиска.
    Возвращает строку-токен или None при ошибке.
    """
    okvd_clean = okvd.replace(".", "")
    try:
        resp = session.post(
            "https://egrul.nalog.ru/",
            data={
                "query":      "",
                "region":     region,
                "okvedCodes": okvd_clean,
                "vo":         "",
            },
            headers={
                "User-Agent":      _ua(),
                "Accept":          "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "ru-RU,ru;q=0.9",
                "Content-Type":    "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With":"XMLHttpRequest",
                "Origin":          "https://egrul.nalog.ru",
                "Referer":         "https://egrul.nalog.ru/",
            },
            timeout=15,
        )

        if resp.status_code == 400:
            # Пробуем с ОКВЭД без точки + явный query
            resp = session.post(
                "https://egrul.nalog.ru/",
                data={
                    "query":      okvd_clean,
                    "region":     region,
                    "okvedCodes": "",
                    "vo":         "ul",
                },
                headers={
                    "User-Agent":      _ua(),
                    "Accept":          "application/json, text/javascript, */*; q=0.01",
                    "Content-Type":    "application/x-www-form-urlencoded; charset=UTF-8",
                    "X-Requested-With":"XMLHttpRequest",
                    "Origin":          "https://egrul.nalog.ru",
                    "Referer":         "https://egrul.nalog.ru/",
                },
                timeout=15,
            )

        if resp.status_code != 200:
            log.warning(f"ЕГРЮЛ POST: HTTP {resp.status_code} (регион={region}, ОКВЭД={okvd})")
            return None

        data = resp.json()
        token = data.get("t")
        if not token:
            log.warning(f"ЕГРЮЛ: нет токена. Ответ: {str(data)[:200]}")
        return token

    except Exception as e:
        log.warning(f"ЕГРЮЛ POST ошибка ({region}/{okvd}): {e}")
        return None


def _egrul_fetch_results(session: requests.Session, token: str, page: int = 1) -> list[dict]:
    """GET /search-result?t=TOKEN → список компаний."""
    try:
        resp = session.get(
            "https://egrul.nalog.ru/search-result",
            params={"t": token, "r": "y", "p": page},
            headers={
                "User-Agent":      _ua(),
                "Accept":          "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With":"XMLHttpRequest",
                "Referer":         "https://egrul.nalog.ru/",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        rows = data.get("rows", [])
        companies = []
        for row in rows:
            inn = row.get("i", "")   # ИНН
            if not inn:
                continue
            companies.append({
                "inn":       inn,
                "name":      row.get("n", ""),
                "ogrn":      row.get("o", ""),
                "reg_date":  row.get("r", ""),
                "status":    row.get("s", ""),
            })
        return companies

    except Exception as e:
        log.debug(f"ЕГРЮЛ GET results ошибка: {e}")
        return []


def search_by_okvd_region(session: requests.Session, region: str, okvd: str) -> list[dict]:
    """Полный поиск по ОКВЭД + регион (несколько страниц)."""
    token = _egrul_search_token(session, region, okvd)
    if not token:
        return []

    _sleep(0.8, 1.5)
    companies = []

    for page in range(1, MAX_PAGES_PER_QUERY + 1):
        rows = _egrul_fetch_results(session, token, page=page)
        if not rows:
            break
        for c in rows:
            c["okvd_search"] = okvd
            c["region_name"] = REGIONS.get(region, region)
        companies.extend(rows)
        if len(rows) < 20:   # Последняя страница
            break
        _sleep(0.5, 1.2)

    return companies


# ─────────────────────────────────────────────────────────
# ГИР БО (bo.nalog.ru) — финансовые отчёты
# ─────────────────────────────────────────────────────────

def get_financials(inn: str, session: requests.Session) -> dict:
    """
    Получает финансовые данные компании из ГИР БО.

    Возвращает:
        revenue              — выручка (руб/год)
        cost_of_sales        — себестоимость (руб/год)
        net_profit           — чистая прибыль (руб/год)
        electricity_proxy    — прокси расходов на электро (руб/мес)
        data_year            — год отчётности
    """
    result = {
        "revenue": 0,
        "cost_of_sales": 0,
        "net_profit": 0,
        "electricity_proxy": 0,
        "data_year": "",
    }

    try:
        # 1. Найти организацию по ИНН
        r = session.get(
            "https://bo.nalog.ru/nbo/organizations/search",
            params={"query": inn, "page": 0, "size": 1},
            headers={
                "User-Agent": _ua(),
                "Accept":     "application/json",
                "Referer":    "https://bo.nalog.ru/",
            },
            timeout=15,
        )
        if r.status_code != 200:
            return result

        content = r.json().get("content", [])
        if not content:
            return result

        org_id = content[0].get("id")
        if not org_id:
            return result

        _sleep(0.4, 0.9)

        # 2. Получить финансовую отчётность (пробуем последние три года)
        for year in [2023, 2022, 2021]:
            r2 = session.get(
                f"https://bo.nalog.ru/nbo/organizations/{org_id}/bfo/",
                params={"year": year},
                headers={
                    "User-Agent": _ua(),
                    "Accept":     "application/json",
                    "Referer":    f"https://bo.nalog.ru/companies/{org_id}/accounting-report/",
                },
                timeout=15,
            )
            if r2.status_code != 200:
                continue

            bfo = r2.json()
            if not bfo:
                continue

            # Берём первый отчёт
            report = bfo[0] if isinstance(bfo, list) else bfo

            # Попытка достать данные из разных структур ответа
            rev_report = report.get("revenueReport", report)

            def _val(key):
                v = rev_report.get(key) or report.get(key) or 0
                try:
                    return abs(float(v)) * 1000   # ГИР БО отдаёт тысячи рублей
                except (TypeError, ValueError):
                    return 0

            revenue       = _val("revenue")
            cost_of_sales = _val("costOfSales")
            net_profit    = _val("netProfit")

            if revenue == 0 and cost_of_sales == 0:
                continue

            # Прокси электроэнергии:
            # для ЦОД/хостинга электроэнергия ≈ 40% себестоимости
            # если нет себестоимости — 15% выручки
            if cost_of_sales > 0:
                electricity_proxy = cost_of_sales * 0.40 / 12
            elif revenue > 0:
                electricity_proxy = revenue * 0.15 / 12
            else:
                electricity_proxy = 0

            result.update({
                "revenue":           revenue,
                "cost_of_sales":     cost_of_sales,
                "net_profit":        net_profit,
                "electricity_proxy": electricity_proxy,
                "data_year":         str(year),
            })
            break   # Нашли данные — выходим

    except Exception as e:
        log.debug(f"ГИР БО ошибка ИНН {inn}: {e}")

    return result


# ─────────────────────────────────────────────────────────
# ОСНОВНОЙ ЗАПУСК
# ─────────────────────────────────────────────────────────

def run(okveds=None, regions=None):
    okveds  = okveds  or HOSTING_OKVEDS
    regions = regions or list(REGIONS.keys())

    egrul_session = _make_egrul_session()
    bo_session    = requests.Session()

    all_companies: list[dict] = []
    seen_inns: set[str]       = set()

    # ── Шаг 1: сбор ИНН по ОКВЭД + регионам ──────────────────────────
    log.info(f"Шаг 1: поиск компаний ({len(okveds)} ОКВЭД × {len(regions)} регионов)")

    for okvd in okveds:
        for region in regions:
            log.info(f"  → ОКВЭД {okvd} / {REGIONS.get(region, region)}")
            companies = search_by_okvd_region(egrul_session, region, okvd)
            log.info(f"    Найдено: {len(companies)}")

            for c in companies:
                inn = c.get("inn", "")
                if inn and inn not in seen_inns:
                    seen_inns.add(inn)
                    all_companies.append(c)

            _sleep(2.0, 4.0)

    log.info(f"Итого уникальных ИНН: {len(all_companies)}")
    if not all_companies:
        log.error("Компании не найдены. Проверьте подключение к egrul.nalog.ru.")
        return None

    # ── Шаг 2: финансовые данные ──────────────────────────────────────
    log.info("Шаг 2: загрузка финансовых данных из ГИР БО (bo.nalog.ru)...")

    filtered: list[dict] = []

    for company in tqdm(all_companies, desc="Финансы"):
        inn = company.get("inn", "")
        if not inn:
            continue

        fin = get_financials(inn, bo_session)
        company.update(fin)

        # Фильтр: прокси электроэнергии > 10 млн/мес
        proxy = company.get("electricity_proxy", 0)
        revenue = company.get("revenue", 0)

        passes = (
            proxy   >= ELECTRICITY_MONTHLY_THRESHOLD
            or revenue >= REVENUE_PROXY_THRESHOLD
        )

        if passes:
            filtered.append(company)

        _sleep(0.4, 1.0)

    log.info(f"После фильтра электроэнергии: {len(filtered)} из {len(all_companies)}")

    # ── Шаг 3: сохранение ─────────────────────────────────────────────
    Path("output").mkdir(exist_ok=True)
    today = date.today()

    out_list = filtered if filtered else all_companies
    df = pd.DataFrame(out_list)

    # Убираем ликвидированные
    if "status" in df.columns:
        df = df[~df["status"].str.lower().str.contains("ликвид|закрыт|недейств", na=False)]

    df = df.drop_duplicates(subset=["inn"]) if "inn" in df.columns else df

    csv_path = f"output/hosting_{today}.csv"
    inn_path = f"output/inns_hosting_{today}.txt"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    if "inn" in df.columns:
        df[df["inn"].notna() & (df["inn"] != "")]["inn"].to_csv(
            inn_path, index=False, header=False
        )

    print(f"\n{'='*60}")
    print(f"  ХОСТИНГ / ЦОД  (электроэнергия > 10 млн руб/мес)")
    print(f"  Найдено компаний:     {len(all_companies)}")
    print(f"  Прошли фильтр:        {len(filtered)}")
    print(f"  CSV:  {csv_path}")
    print(f"  ИНН:  {inn_path}")
    print(f"{'='*60}\n")

    return df


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Поиск хостинговых компаний с большими расходами на электроэнергию")
    ap.add_argument("--okveds",  nargs="+", default=None, help="ОКВЭД коды через пробел")
    ap.add_argument("--regions", nargs="+", default=None, help="Коды регионов ЕГРЮЛ (38 24 19 ...)")
    args = ap.parse_args()

    run(okveds=args.okveds, regions=args.regions)
