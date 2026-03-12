# src/scoring.py — скоринговая модель

def calculate_mining_score(financials: dict, electricity: float) -> tuple[int, list]:
    """
    Считает скоринговый балл от 0 до 100.
    Возвращает (балл, список сработавших признаков).
    """
    score = 0
    triggers = []

    rev = financials.get("revenue", 0)
    costs = financials.get("cost_of_sales", 0)
    fa = financials.get("fixed_assets", 0)
    bal = financials.get("balance_total", 0)
    employees = financials.get("employees", 0)
    net_profit = financials.get("net_profit", 0)

    # ── Расходы на электроэнергию ──────────────────
    if electricity >= 50_000_000:
        score += 30
        triggers.append(f"ЭЭ > 50 млн (+30): {electricity/1e6:.1f} млн руб.")
    elif electricity >= 10_000_000:
        score += 20
        triggers.append(f"ЭЭ > 10 млн (+20): {electricity/1e6:.1f} млн руб.")
    elif electricity >= 5_000_000:
        score += 10
        triggers.append(f"ЭЭ > 5 млн (+10): {electricity/1e6:.1f} млн руб.")
    elif electricity > 0:
        score += 5
        triggers.append(f"ЭЭ найдена (+5): {electricity/1e6:.1f} млн руб.")

    # ── Высокая себестоимость / выручка ───────────
    if rev > 0:
        cost_ratio = costs / rev
        if cost_ratio > 0.8:
            score += 20
            triggers.append(f"Себест./Выручка > 80% (+20): {cost_ratio:.0%}")
        elif cost_ratio > 0.7:
            score += 15
            triggers.append(f"Себест./Выручка > 70% (+15): {cost_ratio:.0%}")
        elif cost_ratio > 0.6:
            score += 8
            triggers.append(f"Себест./Выручка > 60% (+8): {cost_ratio:.0%}")

    # ── Высокая доля ОС в балансе ─────────────────
    if bal > 0:
        fa_ratio = fa / bal
        if fa_ratio > 0.6:
            score += 20
            triggers.append(f"ОС/Баланс > 60% (+20): {fa_ratio:.0%}")
        elif fa_ratio > 0.4:
            score += 12
            triggers.append(f"ОС/Баланс > 40% (+12): {fa_ratio:.0%}")

    # ── Мало сотрудников при высокой выручке ──────
    if employees > 0 and rev > 0:
        rev_per_emp = rev / employees
        if rev_per_emp > 20_000_000:
            score += 15
            triggers.append(f"Выручка/сотрудник > 20 млн (+15): {rev_per_emp/1e6:.1f} млн")
        elif rev_per_emp > 10_000_000:
            score += 8
            triggers.append(f"Выручка/сотрудник > 10 млн (+8): {rev_per_emp/1e6:.1f} млн")
    elif employees == 0 and rev > 5_000_000:
        score += 10
        triggers.append("Нет сотрудников при выручке > 5 млн (+10)")

    # ── Низкая чистая прибыль при высокой выручке ─
    # (характерно: деньги уходят в ЭЭ и амортизацию)
    if rev > 10_000_000 and net_profit < rev * 0.05:
        score += 5
        triggers.append("Низкая рентабельность (<5%) (+5)")

    return min(score, 100), triggers


def get_priority_label(score: int) -> str:
    if score >= 70:
        return "🔴 Горячий"
    elif score >= 40:
        return "🟡 Тёплый"
    else:
        return "🟢 Холодный"
