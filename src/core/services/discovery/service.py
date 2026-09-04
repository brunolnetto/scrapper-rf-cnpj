"""
Federal Revenue Directory Scraper

Discovers available data periods from the Federal Revenue CNPJ Nextcloud share
via WebDAV PROPFIND, replacing the old HTML-scraping approach.
"""

import re
import base64
import requests
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from xml.etree import ElementTree as ET

from src.setup.config import AppConfig
from src.setup.logging import logger

# ---------------------------------------------------------------------------
# Nextcloud WebDAV constants
# ---------------------------------------------------------------------------
_WEBDAV_BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav/"
_SHARE_TOKEN  = "YggdBLfdninEJX9"
_WEBDAV_AUTH  = base64.b64encode(f"{_SHARE_TOKEN}:".encode()).decode()
_WEBDAV_HEADERS = {
    "Authorization": f"Basic {_WEBDAV_AUTH}",
    "Depth": "1",
    "User-Agent": "CNPJ-ETL-Pipeline/1.0",
}
_NS = {"d": "DAV:"}


@dataclass
class DataPeriod:
    """Represents an available data period from Federal Revenue."""
    year: int
    month: int
    directory_name: str
    last_modified: str
    url: str

    @property
    def period_str(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def is_current_month(self) -> bool:
        now = datetime.now()
        return self.year == now.year and self.month == now.month

    def __str__(self):
        return f"DataPeriod({self.period_str})"

    def __repr__(self):
        return self.__str__()


@dataclass
class PeriodFileInfo:
    """Represents a file within a specific data period from Federal Revenue."""
    filename: str
    updated_at: datetime
    file_size: int
    download_url: str

    def __str__(self):
        return f"PeriodFileInfo({self.filename})"


class FederalRevenueDiscoveryService:
    """
    Service for discovering available data periods from the Federal Revenue
    Nextcloud share via WebDAV PROPFIND.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.period_pattern = re.compile(r"^(\d{4})-(\d{2})/$")
        self.timeout = 60

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _propfind(self, url: str) -> ET.Element:
        """Issue a WebDAV PROPFIND and return the parsed XML root."""
        response = requests.request(
            "PROPFIND",
            url,
            headers=_WEBDAV_HEADERS,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return ET.fromstring(response.content)

    def _href_to_name(self, href: str) -> str:
        """Extract the last path component from a WebDAV href."""
        return href.rstrip("/").split("/")[-1]

    # ------------------------------------------------------------------
    # Public API (mirrors the old HTML-scraping interface)
    # ------------------------------------------------------------------

    def fetch_directory_listing(self) -> str:
        """
        Legacy compatibility shim.  Returns a sentinel string; the real work
        is done by parse_directory_listing via WebDAV.
        """
        return "__webdav__"

    def parse_directory_listing(self, _html_content: str) -> List[DataPeriod]:
        """
        Discover available periods via WebDAV PROPFIND on the root share.

        Args:
            _html_content: Ignored (kept for interface compatibility).

        Returns:
            List of DataPeriod objects sorted newest-first.
        """
        root = self._propfind(_WEBDAV_BASE)
        periods: List[DataPeriod] = []

        for response in root.findall("d:response", _NS):
            href = response.findtext("d:href", default="", namespaces=_NS)
            name = self._href_to_name(href)

            match = self.period_pattern.match(name + "/")
            if not match:
                continue

            year  = int(match.group(1))
            month = int(match.group(2))
            if not (1 <= month <= 12):
                continue

            last_modified = response.findtext(
                "d:propstat/d:prop/d:getlastmodified",
                default="",
                namespaces=_NS,
            )

            period_url = f"{_WEBDAV_BASE}{name}/"
            periods.append(DataPeriod(
                year=year,
                month=month,
                directory_name=name,
                last_modified=last_modified,
                url=period_url,
            ))

        periods.sort(key=lambda p: (p.year, p.month), reverse=True)
        logger.info(f"Discovered {len(periods)} periods via WebDAV")
        return periods

    def discover_available_periods(self) -> List[DataPeriod]:
        html_content = self.fetch_directory_listing()
        return self.parse_directory_listing(html_content)

    def get_latest_period(self) -> Optional[DataPeriod]:
        periods = self.discover_available_periods()
        return periods[0] if periods else None

    def find_period(self, year: int, month: int) -> Optional[DataPeriod]:
        for period in self.discover_available_periods():
            if period.year == year and period.month == month:
                return period
        return None

    def get_periods_since(self, year: int, month: int) -> List[DataPeriod]:
        target = (year, month)
        return [p for p in self.discover_available_periods()
                if (p.year, p.month) >= target]

    def get_periods_for_year(self, year: int) -> List[DataPeriod]:
        return [p for p in self.discover_available_periods() if p.year == year]

    def get_summary(self, periods: List[DataPeriod]) -> Dict:
        if not periods:
            return {"total_periods": 0, "date_range": None,
                    "latest_period": None, "years_covered": [], "periods": []}
        latest = periods[0]
        oldest = periods[-1]
        return {
            "total_periods":  len(periods),
            "date_range":     f"{oldest.period_str} to {latest.period_str}",
            "latest_period":  latest.period_str,
            "years_covered":  sorted({p.year for p in periods}),
            "periods":        [p.period_str for p in periods[:10]],
        }

    def validate_period_availability(self, year: int, month: int) -> bool:
        return self.find_period(year, month) is not None

    def scrape_period_files(self, year: int, month: int) -> List[PeriodFileInfo]:
        period = self.find_period(year, month)
        if not period:
            raise ValueError(f"Period {year:04d}-{month:02d} is not available")
        return self.scrape_files_from_url(period.url)

    def scrape_files_from_url(self, period_url: str) -> List[PeriodFileInfo]:
        """
        List ZIP files in a period folder via WebDAV PROPFIND.

        Args:
            period_url: WebDAV URL of the period directory.

        Returns:
            List of PeriodFileInfo for every .zip found.
        """
        logger.info(f"Scraping files via WebDAV from: {period_url}")

        root = self._propfind(period_url)
        files_info: List[PeriodFileInfo] = []

        import pytz
        try:
            tz_str = self.config.pipeline.data_source.timezone
        except AttributeError:
            tz_str = "America/Sao_Paulo"
        tz = pytz.timezone(tz_str)

        for response in root.findall("d:response", _NS):
            href = response.findtext("d:href", default="", namespaces=_NS)
            filename = self._href_to_name(href)

            if not filename.endswith(".zip"):
                continue

            # File size (content-length)
            size_str = response.findtext(
                "d:propstat/d:prop/d:getcontentlength",
                default="0",
                namespaces=_NS,
            )
            try:
                file_size = int(size_str)
            except ValueError:
                file_size = 0

            # Last modified
            lm_str = response.findtext(
                "d:propstat/d:prop/d:getlastmodified",
                default="",
                namespaces=_NS,
            )
            updated_at = self._parse_rfc1123(lm_str, filename, tz)

            # Download URL — direct WebDAV path works for GET as well
            download_url = f"{period_url}{filename}"

            files_info.append(PeriodFileInfo(
                filename=filename,
                updated_at=updated_at,
                file_size=file_size,
                download_url=download_url,
            ))

        logger.info(f"Found {len(files_info)} ZIP files in {period_url}")
        return files_info

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_rfc1123(self, date_str: str, filename: str, tz) -> datetime:
        """Parse RFC 1123 date returned by WebDAV (e.g. 'Mon, 01 Jan 2024 00:00:00 GMT')."""
        from email.utils import parsedate_to_datetime
        try:
            dt = parsedate_to_datetime(date_str)
            return dt.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)
        except Exception:
            logger.warning(f"Could not parse date '{date_str}' for {filename}, using now()")
            return datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def create_discovery_service(config: AppConfig) -> FederalRevenueDiscoveryService:
    return FederalRevenueDiscoveryService(config)
