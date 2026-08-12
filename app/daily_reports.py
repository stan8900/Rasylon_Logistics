from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class DailyReportStats:
    user_id: int
    report_date: date
    deliveries: int
    campaign_starts: int
    active_now: bool
    target_count: int
    sent_total: int
    last_sent_at: Optional[str]
    last_error: Optional[str]


def parse_event_datetime(value: Any, timezone: ZoneInfo) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone)
    return parsed.astimezone(timezone)


def report_window_for_date(report_date: date, timezone: ZoneInfo) -> tuple[datetime, datetime]:
    start = datetime.combine(report_date, time.min, tzinfo=timezone)
    return start, start + timedelta(days=1)


def collect_report_user_ids(data: Dict[str, Any]) -> List[int]:
    user_ids: Set[int] = set()
    for raw_user_id in (data.get("auto") or {}).keys():
        try:
            user_ids.add(int(raw_user_id))
        except (TypeError, ValueError):
            continue
    for payment in (data.get("payments") or {}).values():
        try:
            user_ids.add(int(payment.get("user_id")))
        except (TypeError, ValueError):
            continue
    for raw_user_id in (data.get("sessions") or {}).keys():
        try:
            user_ids.add(int(raw_user_id))
        except (TypeError, ValueError):
            continue
    return sorted(user_ids)


def build_delivery_bar(deliveries: int, target_count: int, width: int = 10) -> str:
    if target_count <= 0:
        filled = width if deliveries > 0 else 0
    else:
        filled = min(width, round((deliveries / target_count) * width))
    filled = max(0, min(width, filled))
    return "█" * filled + "░" * (width - filled)


def calculate_daily_report_stats(
    *,
    user_id: int,
    report_date: date,
    timezone: ZoneInfo,
    auto_data: Optional[Dict[str, Any]],
    delivery_events: Iterable[Dict[str, Any]],
    campaign_events: Iterable[Dict[str, Any]],
) -> DailyReportStats:
    start, end = report_window_for_date(report_date, timezone)
    deliveries = 0
    campaign_starts = 0

    for event in delivery_events:
        if int(event.get("user_id") or 0) != int(user_id):
            continue
        delivered_at = parse_event_datetime(event.get("delivered_at"), timezone)
        if delivered_at and start <= delivered_at < end:
            deliveries += int(event.get("sent_count") or 0)

    for event in campaign_events:
        if int(event.get("user_id") or 0) != int(user_id):
            continue
        started_at = parse_event_datetime(event.get("started_at"), timezone)
        if started_at and start <= started_at < end:
            campaign_starts += 1

    auto = auto_data or {}
    stats = auto.get("stats") or {}
    return DailyReportStats(
        user_id=int(user_id),
        report_date=report_date,
        deliveries=deliveries,
        campaign_starts=campaign_starts,
        active_now=bool(auto.get("is_enabled")),
        target_count=len(auto.get("target_chat_ids") or []),
        sent_total=int(stats.get("sent_total") or 0),
        last_sent_at=stats.get("last_sent_at"),
        last_error=stats.get("last_error"),
    )


def format_daily_report(stats: DailyReportStats, timezone: ZoneInfo) -> str:
    date_text = stats.report_date.strftime("%d.%m.%Y")
    status = "активна" if stats.active_now else "остановлена"
    chart = build_delivery_bar(stats.deliveries, stats.target_count)
    last_sent = parse_event_datetime(stats.last_sent_at, timezone)
    last_sent_text = last_sent.strftime("%d.%m.%Y %H:%M") if last_sent else "нет"

    lines = [
        f"📊 <b>Ежедневный отчёт по рассылке за {date_text}</b>",
        "",
        f"Отправлено сообщений: <b>{stats.deliveries}</b>",
        f"Запусков рассылки: <b>{stats.campaign_starts}</b>",
        f"Выбранных групп: <b>{stats.target_count}</b>",
        f"Статус сейчас: <b>{status}</b>",
        f"Всего отправлено за всё время: <b>{stats.sent_total}</b>",
        f"Последняя отправка: {last_sent_text}",
        "",
        f"<code>{chart}</code>",
    ]
    return "\n".join(lines)


async def build_daily_report_text(storage: Any, user_id: int, report_date: date, timezone: ZoneInfo) -> str:
    data = await storage.get_data()
    delivery_events = await storage.list_auto_delivery_events()
    campaign_events = await storage.list_auto_campaign_events()
    auto_data = (data.get("auto") or {}).get(str(user_id))
    stats = calculate_daily_report_stats(
        user_id=user_id,
        report_date=report_date,
        timezone=timezone,
        auto_data=auto_data,
        delivery_events=delivery_events,
        campaign_events=campaign_events,
    )
    return format_daily_report(stats, timezone)
