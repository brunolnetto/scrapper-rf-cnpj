"""
router.py — CNPJ pipeline NotificationRouter factory.

Wires sqldim's NotificationRouter with an e-mail channel:

* **SMTP e-mail**: receives P2+ events (all failures/warnings).

The channel is optional — the router is always returned, but the channel is
only added when ``smtp_host`` and at least one recipient are present in the
config.  This makes the factory safe in development (no env vars required)
and loud in production.

Channel implementation uses stdlib ``smtplib`` (STARTTLS by default) with
no additional dependencies beyond what the project already requires.

Usage::

    router = make_notification_router(config)
    router.route(NotificationEvent(
        event_type="pipeline_crash",
        severity=Severity.P1,
        layer=Layer.GOLD,
        details={"error": str(exc)},
    ))
"""

from __future__ import annotations

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from sqldim.notifications import (
    NotificationRouter,
    NotificationChannel,
    NotificationEvent,
    NotificationRule,
    Severity,
)
from sqldim.medallion import Layer

from ....setup.logging import logger


# ---------------------------------------------------------------------------
# Channel implementations
# ---------------------------------------------------------------------------

class SmtpEmailChannel(NotificationChannel):
    """Sends a notification e-mail via SMTP (STARTTLS by default)."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        from_addr: str,
        to_addrs: list[str],
        use_tls: bool = True,
        timeout_s: int = 10,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._from_addr = from_addr
        self._to_addrs = to_addrs
        self._use_tls = use_tls
        self._timeout = timeout_s

    def dispatch(self, event: NotificationEvent) -> bool:
        severity_label = event.severity.name
        subject = f"[{severity_label}] CNPJ Pipeline — {event.event_type}"

        body_lines = [
            f"Severity : {severity_label}",
            f"Event    : {event.event_type}",
            f"Layer    : {event.layer.value}",
        ]
        if event.contract_id:
            body_lines.append(f"Contract : {event.contract_id}")
        if event.details:
            body_lines.append("")
            body_lines.append("Details:")
            for k, v in list(event.details.items())[:10]:
                body_lines.append(f"  {k}: {str(v)[:200]}")

        msg = MIMEMultipart()
        msg["From"] = self._from_addr
        msg["To"] = ", ".join(self._to_addrs)
        msg["Subject"] = subject
        msg.attach(MIMEText("\n".join(body_lines), "plain"))

        try:
            with smtplib.SMTP(self._host, self._port, timeout=self._timeout) as smtp:
                if self._use_tls:
                    smtp.starttls()
                if self._user:
                    smtp.login(self._user, self._password)
                smtp.sendmail(self._from_addr, self._to_addrs, msg.as_string())
            return True
        except Exception as exc:
            logger.warning(f"[EmailChannel] Dispatch failed: {exc}")
            return False


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def make_notification_router(config) -> NotificationRouter:
    """Build a NotificationRouter from application config.

    Reads optional ``config.pipeline.notifications`` for SMTP credentials.
    If the attribute is absent (e.g. dev environment with no notifications
    configured) an empty router is returned — ``router.route(event)`` will
    produce an empty results list rather than raising.

    Routing rules:
    * ``email`` channel: P2+ events (all failures/warnings).
    """
    router = NotificationRouter()

    notif_cfg = getattr(
        getattr(config, "pipeline", None), "notifications", None
    )

    smtp_host: Optional[str] = getattr(notif_cfg, "smtp_host", None)
    smtp_to: list[str] = getattr(notif_cfg, "smtp_to", None) or []

    if smtp_host and smtp_to:
        smtp_user = getattr(notif_cfg, "smtp_user", "") or ""
        channel = SmtpEmailChannel(
            host=smtp_host,
            port=getattr(notif_cfg, "smtp_port", 587),
            user=smtp_user,
            password=getattr(notif_cfg, "smtp_password", "") or "",
            from_addr=getattr(notif_cfg, "smtp_from", None) or smtp_user,
            to_addrs=smtp_to,
            use_tls=getattr(notif_cfg, "use_tls", True),
        )
        router.add_channel("email", channel)
        router.add_rule(NotificationRule(
            name="p2plus_email",
            event_types=["*"],
            layers=None,
            min_severity=Severity.P2,
            channels=["email"],
        ))
        logger.info("[Notifications] Email channel registered (%d recipient(s))", len(smtp_to))
    else:
        logger.debug("[Notifications] Email not configured (smtp_host or smtp_to missing)")

    return router
