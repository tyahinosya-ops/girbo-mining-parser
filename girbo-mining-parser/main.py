#!/usr/bin/env python3
# main.py — точка входа

import argparse
import logging
from src.parser import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Mining Parser — поиск компаний по двум категориям:\n"
            "  hosting — хостинг/ЦОД с расходами на электроэнергию > 10 млн руб/мес\n"
            "  mining  — майнеры по лизинговым договорам на Федресурсе"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--category",
        choices=["hosting", "mining"],
        default="hosting",
        help="Категория поиска: hosting (хостинг/ЦОД) или mining (майнеры). По умолчанию: hosting",
    )
    parser.add_argument(
        "--mode",
        choices=["api", "file"],
        default="api",
        help="Источник ИНН: api (ЕГРЮЛ / Федресурс) или file (из файла). По умолчанию: api",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Путь к файлу с ИНН (TXT или CSV). Обязателен при --mode file",
    )
    parser.add_argument(
        "--min-ee",
        type=float,
        default=10_000_000,
        dest="min_electricity",
        help="Минимальные расходы на ЭЭ в руб/месяц (только для --category hosting). По умолчанию: 10 000 000",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2023,
        help="Год отчётности для ГИР БО. По умолчанию: 2023",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Путь к выходному CSV (по умолчанию: output/mining_leads_<категория>_<дата>.csv)",
    )

    args = parser.parse_args()

    if args.mode == "file" and not args.input:
        parser.error("--mode file требует --input путь/к/файлу.txt")

    run(
        mode=args.mode,
        category=args.category,
        input_file=args.input,
        min_electricity=args.min_electricity,
        year=args.year,
        output_file=args.output,
    )


if __name__ == "__main__":
    main()
