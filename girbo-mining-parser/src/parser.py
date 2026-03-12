# src/parser.py — основной парсер

import time
import random
import logging
import requests
from pathlib import Path
import pandas as pd
from datetime import date

from config import (
    HOSTING_OKVEDS,
    TARGET_REGIONS,
    REQUEST_DELAY,
    REQUEST_TIMEOUT,
    MIN_REVENUE,
    MIN_SCORE,
    MIN_ELECTRICITY_EXPENSE,
    OUTPUT_DIR,
    DEFAULT_OUTPUT_FILE,
)
from src.extractor import get_report_from_girbo, extract_electricity_expenses, extract_key_financials
from src.scoring import calculate_mining_score, get_priority_label
from src.fedresurs import get_inns_from_fedresurs

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]


def _ua():
    return random.choice(USER_AGENTS)


def _sleep(lo=None, hi=None):
    lo = lo or REQUEST_DELAY
    hi = hi or REQUEST_DELAY * 2.5
    time.sleep(random.uniform(lo, hi))


# ─────────────────────────────────────────────────────────
# ЕГРЮЛ — двухшаговый API
# ─────────────────────────────────────────────────────────

def _init_egrul_session() -> requests.Session:
    """Инициализирует сессию с egrul.nalog.ru (получает cookies)."""
    session = requests.Session()
    try:
        session.get(
            "https://egrul.nalog.ru/",
            headers={
                "User-Agent":    _ua(),
                "Accept":        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            },
            timeout=REQUEST_TIMEOUT,
        )
        _sleep(1.5, 3.0)
        log.info("Сессия egrul.nalog.ru инициализирована.")
    except Exception as e:
        log.warning(f"Не удалось инициализировать сессию ЕГРЮЛ: {e}")
    return session


def _egrul_get_token(session: requests.Session, region: str, okvd: str) -> str | None:
    """
    POST /  → возвращает поисковый токен.
    Правильный двухшаговый API ЕГРЮЛ:
      Шаг 1: POST с параметрами → {"t": "token"}
      Шаг 2: GET /search-result?t=token&p=1 → список компаний
    """
    okvd_clean = okvd.replace(".", "")
    base_headers = {
        "User-Agent":       _ua(),
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "Accept-Language":  "ru-RU,ru;q=0.9",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin":           "https://egrul.nalog.ru",
        "Referer":          "https://egrul.nalog.ru/",
    }

    # Пробуем разные варианты формата ОКВЭД
    for payload in [
        {"query": "", "region": region, "okvedCodes": okvd_clean, "vo": ""},
        {"query": "", "region": region, "okvedCodes": okvd,       "vo": ""},
        {"query": "", "region": region, "okvedCodes": okvd_clean, "vo": "ul"},
    ]:
        try:
            resp = session.post(
                "https://egrul.nalog.ru/",
                data=payload,
                headers=base_headers,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                token = resp.json().get("t")
                if token:
                    return token
        except Exception as e:
            log.debug(f"ЕГРЮЛ POST ошибка: {e}")
        _sleep(0.5, 1.0)

    log.warning(f"ЕГРЮЛ: токен не получен (регион={region}, ОКВЭД={okvd})")
    return None


def _egrul_fetch_page(session: requests.Session, token: str, page: int = 1) -> tuple[list[dict], int]:
    """GET /search-result?t=TOKEN&p=PAGE → (компании, всего)."""
    try:
        resp = session.get(
            "https://egrul.nalog.ru/search-result",
            params={"t": token, "r": "y", "p": page},
            headers={
                "User-Agent":       _ua(),
                "Accept":           "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer":          "https://egrul.nalog.ru/",
            },
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return [], 0

        data  = resp.json()
        rows  = data.get("rows", [])
        total = data.get("cnt", len(rows))

        companies = []
        for row in rows:
            inn = row.get("i", "").strip()
            if inn:
                companies.append({
                    "inn":      inn,
                    "name":     row.get("n", ""),
                    "ogrn":     row.get("o", ""),
                    "reg_date": row.get("r", ""),
                    "status":   row.get("s", ""),
                })
        return companies, total

    except Exception as e:
        log.debug(f"ЕГРЮЛ /search-result ошибка: {e}")
        return [], 0


def get_inns_from_egrul(okveds: list, regions: list) -> list[str]:
    """Собирает ИНН хостинговых компаний из ЕГРЮЛ по ОКВЭД + регионам."""
    session = _init_egrul_session()
    inns: set[str] = set()

    for okvd in okveds:
        for region in regions:
            log.info(f"  ЕГРЮЛ: ОКВЭД {okvd} / регион {region}")

            token = _egrul_get_token(session, region, okvd)
            if not token:
                continue

            _sleep(0.8, 1.5)

            first_page, total = _egrul_fetch_page(session, token, page=1)
            log.info(f"    Всего: {total}, на стр. 1: {len(first_page)}")

            for c in first_page:
                inns.add(c["inn"])

            # Докачиваем страницы (до 10 страниц по 20 записей)
            total_pages = min((total // 20) + 1, 10)
            for page in range(2, total_pages + 1):
                _sleep(0.5, 1.2)
                more, _ = _egrul_fetch_page(session, token, page=page)
                if not more:
                    break
                for c in more:
                    inns.add(c["inn"])

            _sleep(2.0, 4.0)

    result = list(inns)
    log.info(f"Собрано уникальных ИНН из ЕГРЮЛ: {len(result)}")
    return result


# ─────────────────────────────────────────────────────────
# Загрузка ИНН из файла
# ─────────────────────────────────────────────────────────

def load_inns_from_file(filepath: str) -> list[str]:
    """Загружает ИНН из TXT или CSV файла."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {filepath}")

    if filepath.endswith(".csv"):
        df = pd.read_csv(filepath)
        inn_col = next(
            (c for c in df.columns if "инн" in c.lower() or "inn" in c.lower()),
            df.columns[0],
        )
        inns = df[inn_col].astype(str).str.strip().tolist()
    elif filepath.endswith(".xlsx"):
        df = pd.read_excel(filepath)
        inn_col = next(
            (c for c in df.columns if "инн" in c.lower() or "inn" in c.lower()),
            df.columns[0],
        )
        inns = df[inn_col].astype(str).str.strip().tolist()
    else:
        with open(filepath, encoding="utf-8") as f:
            inns = [line.strip() for line in f if line.strip()]

    valid = [i for i in inns if i.isdigit() and len(i) in (10, 12)]
    log.info(f"Загружено из файла: {len(valid)} ИНН (отфильтровано: {len(inns) - len(valid)})")
    return valid


# ─────────────────────────────────────────────────────────
# Основной пайплайн
# ─────────────────────────────────────────────────────────

def run(
    mode: str = "api",
    category: str = "hosting",
    input_file: str = None,
    min_electricity: float = None,
    year: int = 2023,
    output_file: str = None,
) -> pd.DataFrame:

    min_electricity = min_electricity or MIN_ELECTRICITY_EXPENSE

    # ── Шаг 1: Получение ИНН ──────────────────────────────────────────
    log.info(f"Шаг 1: получение ИНН (режим={mode}, категория={category})")

    if mode == "file" and input_file:
        inns = load_inns_from_file(input_file)
    elif category == "mining":
        # Майнеры: ищем через лизинговые договоры на Федресурсе
        inns = get_inns_from_fedresurs()
    else:
        # Хостинг: ищем по ОКВЭД через ЕГРЮЛ
        inns = get_inns_from_egrul(HOSTING_OKVEDS, TARGET_REGIONS)

    if not inns:
        log.error("Список ИНН пуст — нет данных для анализа.")
        return pd.DataFrame()

    log.info(f"ИНН для анализа бухотчётности: {len(inns)}")

    # ── Шаг 2: Финансовый анализ через ГИР БО ────────────────────────
    log.info("Шаг 2: анализ бухотчётности через ГИР БО (bo.nalog.ru)...")

    results = []
    iterator = tqdm(inns, desc="Анализ отчётности") if HAS_TQDM else inns

    for inn in iterator:
        _sleep(REQUEST_DELAY, REQUEST_DELAY * 2)

        report_data = get_report_from_girbo(inn, year=year)
        if not report_data:
            continue

        electricity = extract_electricity_expenses(report_data)
        financials  = extract_key_financials(report_data)
        score, triggers = calculate_mining_score(financials, electricity)

        if financials["revenue"] < MIN_REVENUE:
            continue

        if category == "hosting":
            # Прокси расходов на электроэнергию: себестоимость × 40% / 12
            proxy_monthly = financials["cost_of_sales"] * 0.40 / 12
            passes = (
                electricity    >= min_electricity
                or proxy_monthly >= min_electricity
                or score         >= MIN_SCORE
            )
        else:
            # Майнеры уже отфильтрованы Федресурсом — берём всех
            passes = True

        if not passes:
            continue

        results.append({
            "ИНН":                 inn,
            "Категория":           "Хостинг" if category == "hosting" else "Майнинг",
            "Компания":            report_data.get("org_name", ""),
            "Полное_наименование": report_data.get("full_name", ""),
            "Регион":              report_data.get("region", ""),
            "ОКВЭД":               report_data.get("okvd_main", ""),
            "Расходы_ЭЭ_руб":     int(electricity),
            "Прокси_ЭЭ_мес_руб":  int(financials["cost_of_sales"] * 0.40 / 12),
            "Выручка_руб":         int(financials["revenue"]),
            "Себестоимость_руб":   int(financials["cost_of_sales"]),
            "ОС_руб":              int(financials["fixed_assets"]),
            "Баланс_руб":          int(financials["balance_total"]),
            "Чистая_прибыль_руб":  int(financials["net_profit"]),
            "Сотрудников":         financials["employees"],
            "Скоринг_майнинг":     score,
            "Приоритет":           get_priority_label(score),
            "Триггеры":            " | ".join(triggers),
        })

    if not results:
        log.warning("Не найдено компаний по заданным критериям.")
        return pd.DataFrame()

    # ── Шаг 3: Сохранение ─────────────────────────────────────────────
    df = pd.DataFrame(results)
    df = df.sort_values("Скоринг_майнинг", ascending=False).reset_index(drop=True)

    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    today  = date.today().strftime("%Y-%m-%d")
    suffix = "hosting" if category == "hosting" else "mining"
    out    = output_file or f"{OUTPUT_DIR}/{DEFAULT_OUTPUT_FILE}_{suffix}_{today}.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")

    inn_path = f"{OUTPUT_DIR}/inns_{suffix}_{today}.txt"
    df[df["ИНН"].notna()]["ИНН"].to_csv(inn_path, index=False, header=False)

    hot  = len(df[df["Приоритет"].str.contains("Горячий",  na=False)])
    warm = len(df[df["Приоритет"].str.contains("Тёплый",   na=False)])
    cold = len(df[df["Приоритет"].str.contains("Холодный", na=False)])

    cat_label = "ХОСТИНГ / ЦОД (расходы ЭЭ > 10 млн/мес)" if category == "hosting" else "МАЙНЕРЫ (лизинг — Федресурс)"
    print(f"\n{'='*60}")
    print(f"  {cat_label}")
    print(f"  Компаний найдено:  {len(df)}")
    print(f"  Горячих лидов:     {hot}")
    print(f"  Тёплых лидов:      {warm}")
    print(f"  Холодных лидов:    {cold}")
    print(f"  CSV:  {out}")
    print(f"  ИНН:  {inn_path}")
    print(f"{'='*60}\n")

    return df
