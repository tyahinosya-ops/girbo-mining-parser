# src/fedresurs.py — поиск майнинговых компаний через Федресурс

import re
import time
import random
import logging
import requests

from config import REQUEST_DELAY, REQUEST_TIMEOUT

log = logging.getLogger(__name__)

# Ключевые слова для поиска лизинга майнинг-оборудования
MINING_KEYWORDS = [
    "ASIC майнер",
    "Antminer",
    "Whatsminer",
    "MicroBT",
    "Bitmain",
    "майнинг оборудование",
    "криптовалюта лизинг",
    "GPU ферма",
    "bitcoin mining",
    "добыча криптовалюты",
    "горнодобывающее оборудование лизинг",
]

MAX_PER_KEYWORD = 200   # Максимум записей на одно ключевое слово

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

INN_RE = re.compile(r"(?:ИНН|inn)[:\s]*(\d{10,12})", re.IGNORECASE)

# Возможные эндпоинты (API Федресурса периодически меняется)
ENDPOINTS = [
    "https://fedresurs.ru/backend/search",
    "https://fedresurs.ru/backend/efrs-messages",
    "https://fedresurs.ru/backend/companies",
]


def _ua():
    return random.choice(USER_AGENTS)


def _sleep(lo=None, hi=None):
    lo = lo or REQUEST_DELAY
    hi = hi or REQUEST_DELAY * 2.5
    time.sleep(random.uniform(lo, hi))


def _extract_inn(item: dict) -> str:
    """Извлекает ИНН из произвольного поля записи Федресурса."""
    for field in ("entityInn", "inn", "companyInn", "participantInn", "debtorInn"):
        val = str(item.get(field, "")).strip()
        if re.fullmatch(r"\d{10,12}", val):
            return val

    # Поиск ИНН в текстовых полях
    text = " ".join(str(item.get(k, "")) for k in (
        "messageText", "text", "title", "description", "entityName", "companyName"
    ))
    m = INN_RE.search(text)
    return m.group(1) if m else ""


def _find_working_endpoint(session: requests.Session, keyword: str) -> str | None:
    """Проверяет доступность эндпоинтов и возвращает первый рабочий."""
    for ep in ENDPOINTS:
        try:
            r = session.get(
                ep,
                params={"searchString": keyword, "limit": 5, "offset": 0},
                headers={"User-Agent": _ua(), "Accept": "application/json", "Referer": "https://fedresurs.ru/"},
                timeout=REQUEST_TIMEOUT,
            )
            if r.status_code == 200:
                return ep
        except Exception:
            continue
    return None


def search_by_keyword(keyword: str, session: requests.Session) -> list[dict]:
    """Ищет сообщения на Федресурсе по ключевому слову, возвращает записи с ИНН."""
    endpoint = _find_working_endpoint(session, keyword)
    if not endpoint:
        log.warning(f"Федресурс: все эндпоинты недоступны для «{keyword}»")
        return []

    results = []
    limit   = 40
    offset  = 0

    while offset < MAX_PER_KEYWORD:
        try:
            r = session.get(
                endpoint,
                params={
                    "searchString": keyword,
                    "limit":        limit,
                    "offset":       offset,
                },
                headers={
                    "User-Agent":    _ua(),
                    "Accept":        "application/json",
                    "Referer":       "https://fedresurs.ru/",
                    "Origin":        "https://fedresurs.ru",
                },
                timeout=REQUEST_TIMEOUT,
            )

            if r.status_code != 200:
                log.warning(f"Федресурс HTTP {r.status_code} (keyword={keyword}, offset={offset})")
                break

            data  = r.json()
            items = (
                data.get("data")
                or data.get("items")
                or data.get("content")
                or (data if isinstance(data, list) else [])
            )
            total = data.get("total", data.get("totalElements", 0)) or len(items)

            if not items:
                break

            for item in items:
                inn = _extract_inn(item)
                if inn:
                    results.append({
                        "inn":          inn,
                        "company_name": item.get("entityName", item.get("companyName", "")),
                        "message_date": item.get("publishedDate", item.get("date", "")),
                        "message_type": item.get("messageType", item.get("type", "")),
                        "keyword":      keyword,
                        "source":       "fedresurs",
                    })

            offset += limit
            if offset >= total:
                break

            _sleep(0.8, 1.5)

        except Exception as e:
            log.warning(f"Федресурс ошибка (keyword={keyword}, offset={offset}): {e}")
            break

    return results


def get_inns_from_fedresurs(keywords: list[str] | None = None) -> list[str]:
    """
    Ищет майнинговые компании на Федресурсе по лизинговым договорам.
    Возвращает список уникальных ИНН.
    """
    keywords = keywords or MINING_KEYWORDS
    session  = requests.Session()

    seen_inns: set[str] = set()

    log.info(f"Федресурс: поиск по {len(keywords)} ключевым словам")

    for kw in keywords:
        log.info(f"  → «{kw}»")
        entries = search_by_keyword(kw, session)
        log.info(f"    Записей: {len(entries)}")

        for entry in entries:
            inn = entry.get("inn", "")
            if inn:
                seen_inns.add(inn)

        _sleep(2.0, 3.5)

    inns = list(seen_inns)
    log.info(f"Федресурс: уникальных ИНН = {len(inns)}")
    return inns
