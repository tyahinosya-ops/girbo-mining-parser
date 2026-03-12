# src/extractor.py — извлечение данных из отчётности ГИРБО

import re
import json
import logging
import time
import requests
from config import (
    ELECTRICITY_KEYWORDS,
    REQUEST_DELAY,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    REPORT_YEAR,
)

log = logging.getLogger(__name__)

GIRBO_BASE = "https://bo.nalog.ru/nbo/organizations"


# ─────────────────────────────────────────────
# HTTP-хелпер с retry
# ─────────────────────────────────────────────

def safe_get(url: str, params: dict = None) -> dict | None:
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                # Rate limit — ждём дольше
                log.warning("Rate limit (429), ожидание 10 сек...")
                time.sleep(10)
            else:
                log.debug(f"HTTP {resp.status_code} для {url}")
                return None
        except requests.exceptions.Timeout:
            log.debug(f"Таймаут (попытка {attempt+1}/{MAX_RETRIES}): {url}")
            time.sleep(REQUEST_DELAY * 2)
        except requests.exceptions.ConnectionError:
            log.debug(f"Ошибка соединения (попытка {attempt+1}/{MAX_RETRIES}): {url}")
            time.sleep(REQUEST_DELAY * 3)
        except Exception as e:
            log.debug(f"Неизвестная ошибка: {e}")
            return None
    return None


# ─────────────────────────────────────────────
# Получение отчётности из ГИРБО
# ─────────────────────────────────────────────

def get_report_from_girbo(inn: str, year: int = REPORT_YEAR) -> dict | None:
    """Загружает бухотчётность компании по ИНН из ГИРБО."""

    # Шаг 1: найти ID организации
    data = safe_get(
        f"{GIRBO_BASE}/search",
        params={"query": inn, "page": 0, "size": 5}
    )
    if not data:
        return None

    orgs = data.get("content", [])
    if not orgs:
        return None

    org = orgs[0]
    org_id = org.get("id")
    if not org_id:
        return None

    # Шаг 2: получить отчётность за год
    report = safe_get(f"{GIRBO_BASE}/{org_id}/bfo/", params={"year": year})
    if not report:
        return None

    return {
        "inn": inn,
        "org_name": org.get("shortName", ""),
        "full_name": org.get("fullName", ""),
        "region": org.get("region", ""),
        "okvd_main": org.get("okved", ""),
        "report": report,
    }


# ─────────────────────────────────────────────
# Извлечение расходов на электроэнергию
# ─────────────────────────────────────────────

def extract_electricity_expenses(report_data: dict) -> float:
    """
    Ищет расходы на ЭЭ в пояснениях к отчётности.
    Возвращает сумму в рублях (ГИРБО хранит в тысячах — умножаем на 1000).
    """
    if not report_data or "report" not in report_data:
        return 0.0

    report_str = json.dumps(report_data["report"], ensure_ascii=False).lower()
    best_match = 0.0

    for keyword in ELECTRICITY_KEYWORDS:
        kw = keyword.lower()
        if kw not in report_str:
            continue

        # Ищем число рядом с ключевым словом (до 150 символов после него)
        pattern = rf"{re.escape(kw)}.{{0,150}}?(\d[\d\s\xa0]*\d)"
        matches = re.findall(pattern, report_str)

        for match in matches:
            try:
                cleaned = match.replace(" ", "").replace("\xa0", "")
                amount = float(cleaned)
                # ГИРБО отдаёт суммы в тысячах рублей
                amount_rub = amount * 1000
                # Игнорируем явно нереалистичные значения
                if 100_000 < amount_rub < 100_000_000_000:
                    best_match = max(best_match, amount_rub)
            except ValueError:
                continue

    return best_match


# ─────────────────────────────────────────────
# Извлечение ключевых финансовых показателей
# ─────────────────────────────────────────────

# Коды строк по РСБУ
FORM_CODES = {
    "2110": "revenue",           # Выручка
    "2120": "cost_of_sales",     # Себестоимость
    "1150": "fixed_assets",      # Основные средства
    "1600": "balance_total",     # Валюта баланса
    "2330": "interest_expense",  # Проценты к уплате
    "2400": "net_profit",        # Чистая прибыль
}


def extract_key_financials(report_data: dict) -> dict:
    """Извлекает ключевые строки из баланса и ОФР."""

    result = {k: 0 for k in FORM_CODES.values()}
    result["employees"] = 0

    if not report_data or "report" not in report_data:
        return result

    try:
        report = report_data["report"]
        periods = report if isinstance(report, list) else [report]

        for period in periods:
            forms = period.get("forms", [])
            for form in forms:
                rows = form.get("rows", [])
                for row in rows:
                    code = str(row.get("code", ""))
                    if code in FORM_CODES:
                        raw = row.get("currentPeriodValue", 0) or 0
                        try:
                            # Переводим из тысяч в рубли
                            result[FORM_CODES[code]] = float(raw) * 1000
                        except (ValueError, TypeError):
                            pass

            # Численность сотрудников (если раскрыта)
            avg_employees = period.get("averageNumberOfEmployees", 0)
            if avg_employees:
                result["employees"] = int(avg_employees)

    except Exception as e:
        log.debug(f"Ошибка парсинга финансов: {e}")

    return result
