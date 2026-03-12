"""
mining_parser.py — поиск майнинговых компаний через лизинговые договоры на Федресурсе

Алгоритм:
  1. Поиск сообщений на fedresurs.ru по ключевым словам (ASIC, майнинг, Antminer ...)
  2. Извлечение ИНН компаний из найденных записей
  3. Проверка активности через dadata.ru (если задан DADATA_TOKEN)
  4. Сохранение активных компаний

Переменные окружения:
    DADATA_TOKEN — API-ключ dadata.ru (рекомендуется, 10k запросов/день бесплатно)

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

# Ключевые слова для поиска лизинговых договоров на майнинг-оборудование
MINING_KEYWORDS = [
    "ASIC майнер",
    "Antminer",
    "Whatsminer",
    "MicroBT",
    "Bitmain",
    "майнинг оборудование",
    "криптовалюта лизинг",
    "GPU ферма",
    "горнодобывающее оборудование лизинг",
    "bitcoin mining",
    "добыча криптовалюты",
]

DADATA_TOKEN = os.environ.get("DADATA_TOKEN", "")

# Максимум записей на одно ключевое слово
MAX_RESULTS_PER_KEYWORD = 200

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

INN_RE = re.compile(r"(?:ИНН|inn)[:\s]*(\d{10,12})", re.IGNORECASE)


def _ua():
    return random.choice(USER_AGENTS)


def _sleep(lo=1.2, hi=2.8):
    time.sleep(random.uniform(lo, hi))


# ─────────────────────────────────────────────────────────
# ФЕДРЕСУРС (fedresurs.ru)
# ─────────────────────────────────────────────────────────

FEDRESURS_HEADERS = {
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Origin":          "https://fedresurs.ru",
    "Referer":         "https://fedresurs.ru/",
}


def _extract_inn_from_item(item: dict) -> str:
    """Извлекает ИНН из любого поля записи Федресурса."""
    # Прямые поля
    for field in ("entityInn", "inn", "companyInn", "participantInn", "debtorInn"):
        val = item.get(field, "")
        if val and re.fullmatch(r"\d{10,12}", str(val).strip()):
            return str(val).strip()

    # Из текстовых полей
    text = " ".join(str(v) for v in (
        item.get("messageText", ""),
        item.get("text", ""),
        item.get("title", ""),
        item.get("description", ""),
        item.get("entityName", ""),
    ))
    m = INN_RE.search(text)
    if m:
        return m.group(1)

    return ""


def search_fedresurs(keyword: str, session: requests.Session) -> list[dict]:
    """
    Ищет сообщения на Федресурсе по ключевому слову.
    Пробует несколько endpoint'ов (API периодически меняется).
    """
    results = []
    limit   = 40
    offset  = 0

    # Возможные эндпоинты
    endpoints = [
        "https://fedresurs.ru/backend/search",
        "https://fedresurs.ru/backend/efrs-messages",
        "https://fedresurs.ru/backend/companies",
    ]

    working_endpoint = None

    for ep in endpoints:
        try:
            r = session.get(
                ep,
                params={
                    "searchString": keyword,
                    "limit":        limit,
                    "offset":       0,
                },
                headers={**FEDRESURS_HEADERS, "User-Agent": _ua()},
                timeout=20,
            )
            if r.status_code == 200:
                working_endpoint = ep
                break
        except Exception:
            continue

    if not working_endpoint:
        log.warning(f"Федресурс недоступен для ключевого слова: {keyword}")
        return []

    # Пагинация
    while offset < MAX_RESULTS_PER_KEYWORD:
        try:
            r = session.get(
                working_endpoint,
                params={
                    "searchString": keyword,
                    "limit":        limit,
                    "offset":       offset,
                },
                headers={**FEDRESURS_HEADERS, "User-Agent": _ua()},
                timeout=20,
            )

            if r.status_code != 200:
                log.warning(f"Федресурс HTTP {r.status_code} (смещение={offset})")
                break

            data = r.json()

            # Данные могут быть в разных ключах
            items = (
                data.get("data")
                or data.get("items")
                or data.get("content")
                or (data if isinstance(data, list) else [])
            )
            total = data.get("total", data.get("totalElements", len(items)))

            if not items:
                break

            for item in items:
                inn = _extract_inn_from_item(item)
                if not inn:
                    continue

                results.append({
                    "inn":           inn,
                    "company_name":  item.get("entityName", item.get("companyName", "")),
                    "message_date":  item.get("publishedDate", item.get("date", "")),
                    "message_type":  item.get("messageType", item.get("type", "")),
                    "keyword":       keyword,
                    "source":        "fedresurs",
                })

            offset += limit
            if offset >= total:
                break

            _sleep(0.8, 1.5)

        except Exception as e:
            log.warning(f"Ошибка Федресурс ({keyword}, offset={offset}): {e}")
            break

    return results


# ─────────────────────────────────────────────────────────
# DADATA — проверка активности компании
# ─────────────────────────────────────────────────────────

def verify_with_dadata(inn: str, session: requests.Session) -> dict:
    """
    Проверяет данные и статус компании через dadata.ru.
    Требует DADATA_TOKEN в переменных окружения.
    """
    info = {
        "active":      True,    # Считаем активной, если нет данных
        "full_name":   "",
        "okvd_main":   "",
        "address":     "",
        "reg_date":    "",
        "employees":   "",
    }

    if not DADATA_TOKEN:
        return info

    try:
        r = session.post(
            "https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party",
            json={"query": inn},
            headers={
                "User-Agent":    _ua(),
                "Content-Type":  "application/json",
                "Accept":        "application/json",
                "Authorization": f"Token {DADATA_TOKEN}",
            },
            timeout=10,
        )

        if r.status_code != 200:
            log.debug(f"dadata HTTP {r.status_code} для ИНН {inn}")
            return info

        suggestions = r.json().get("suggestions", [])
        if not suggestions:
            return info

        s    = suggestions[0]
        data = s.get("data", {})

        state  = data.get("state", {}) or {}
        status = state.get("status", "")

        address_obj = data.get("address") or {}
        okvd_obj    = data.get("okved_type") or {}

        info.update({
            "active":    status == "ACTIVE",
            "full_name": s.get("value", ""),
            "okvd_main": data.get("okved", ""),
            "address":   address_obj.get("value", "") if isinstance(address_obj, dict) else str(address_obj),
            "reg_date":  state.get("registration_date", ""),
            "employees": data.get("employee_count", ""),
        })

    except Exception as e:
        log.debug(f"dadata ошибка ИНН {inn}: {e}")

    return info


# ─────────────────────────────────────────────────────────
# ОСНОВНОЙ ЗАПУСК
# ─────────────────────────────────────────────────────────

def run(keywords=None):
    keywords = keywords or MINING_KEYWORDS

    fed_session    = requests.Session()
    verify_session = requests.Session()

    all_entries: list[dict] = []
    seen_inns: set[str]     = set()

    # ── Шаг 1: поиск на Федресурсе ────────────────────────────────────
    log.info(f"Шаг 1: поиск на Федресурсе по {len(keywords)} ключевым словам")

    for kw in keywords:
        log.info(f"  → «{kw}»")
        entries = search_fedresurs(kw, fed_session)
        log.info(f"    Найдено записей: {len(entries)}")

        for entry in entries:
            inn = entry.get("inn", "")
            if inn and inn not in seen_inns:
                seen_inns.add(inn)
                all_entries.append(entry)

        _sleep(2.0, 3.5)

    log.info(f"Уникальных ИНН с Федресурса: {len(all_entries)}")

    if not all_entries:
        log.error("Ничего не найдено на Федресурсе. Возможно, изменился API — проверьте эндпоинты.")
        return None

    # ── Шаг 2: проверка через dadata ──────────────────────────────────
    if DADATA_TOKEN:
        log.info("Шаг 2: проверка активности через dadata.ru...")

        for company in tqdm(all_entries, desc="dadata"):
            inn = company.get("inn", "")
            if not inn:
                continue

            info = verify_with_dadata(inn, verify_session)
            company.update(info)
            _sleep(0.2, 0.5)

        active = [c for c in all_entries if c.get("active", True)]
        log.info(f"Активных: {len(active)} из {len(all_entries)}")
    else:
        log.warning("DADATA_TOKEN не задан — проверка активности пропущена (все считаются активными).")
        active = all_entries

    # ── Шаг 3: сохранение ─────────────────────────────────────────────
    Path("output").mkdir(exist_ok=True)
    today = date.today()

    df = pd.DataFrame(active)
    if "inn" in df.columns:
        df = df.drop_duplicates(subset=["inn"])

    csv_path = f"output/mining_{today}.csv"
    inn_path = f"output/inns_mining_{today}.txt"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    if "inn" in df.columns:
        df[df["inn"].notna() & (df["inn"] != "")]["inn"].to_csv(
            inn_path, index=False, header=False
        )

    print(f"\n{'='*60}")
    print(f"  МАЙНЕРЫ  (лизинг через Федресурс)")
    print(f"  Записей найдено:      {len(all_entries)}")
    print(f"  Активных компаний:    {len(active)}")
    print(f"  CSV:  {csv_path}")
    print(f"  ИНН:  {inn_path}")
    if not DADATA_TOKEN:
        print(f"\n  Совет: задайте DADATA_TOKEN для фильтрации ликвидированных компаний.")
        print(f"  export DADATA_TOKEN=<ваш_ключ>")
    print(f"{'='*60}\n")

    return df


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Поиск майнинговых компаний через лизинговые договоры на Федресурсе")
    ap.add_argument("--keywords", nargs="+", default=None, help="Ключевые слова для поиска")
    args = ap.parse_args()

    run(keywords=args.keywords)
