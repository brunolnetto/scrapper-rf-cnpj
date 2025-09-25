"""
Federal Revenue Directory Scraper

Extracts available data periods from the Federal Revenue CNPJ data directory listing.
This service discovers available year-month combinations for ETL processing.
"""

import re
import requests
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.setup.config import AppConfig
from src.setup.logging import logger


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
        """Return period as YYYY-MM string."""
        return f"{self.year:04d}-{self.month:02d}"
    
    @property
    def is_current_month(self) -> bool:
        """Check if this period is the current month."""
        now = datetime.now()
        return self.year == now.year and self.month == now.month
    
    def __str__(self) -> str:
        """String representation."""
        return f"DataPeriod({self.period_str})"
    
    def __repr__(self) -> str:
        """String representation."""
        return self.__str__()


@dataclass
class PeriodFileInfo:
    """Represents a file within a specific data period from Federal Revenue."""
    filename: str
    updated_at: datetime
    file_size: int
    download_url: str
    
    def __str__(self) -> str:
        """String representation."""
        return f"PeriodFileInfo({self.filename})"


class FederalRevenueDiscoveryService:
    """
    Service for discovering available data periods from Federal Revenue directory.
    
    This service scrapes the directory listing to find available CNPJ data periods,
    which is essential for the ETL pipeline's temporal processing capabilities.
    
    Usage:
        config = get_config()
        discovery = FederalRevenueDiscoveryService(config)
        periods = discovery.discover_available_periods()
    """
    
    def __init__(self, config: AppConfig):
        self.config = config
        # Ensure trailing slash for proper urljoin behavior
        self.base_url = config.pipeline.data_source.base_url.rstrip('/') + '/'
        
        # Regex pattern to match YYYY-MM directory format
        self.period_pattern = re.compile(r'^(\d{4})-(\d{2})/$')
        
        # HTTP session configuration
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CNPJ-ETL-Pipeline/1.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        })
        self.timeout = 30
    
    def fetch_directory_listing(self) -> str:
        """
        Fetch the HTML directory listing from Federal Revenue.
        
        Returns:
            Raw HTML content of the directory listing
            
        Raises:
            requests.RequestException: If request fails
        """
        try:
            response = self.session.get(self.base_url, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch directory listing from {self.base_url}: {e}")
    
    def parse_directory_listing(self, html_content: str) -> List[DataPeriod]:
        """
        Parse HTML directory listing to extract data periods.
        
        Args:
            html_content: Raw HTML content from directory listing
            
        Returns:
            List of DataPeriod objects for available periods
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        periods = []
        
        # Find all table rows with directory links
        for row in soup.find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 4:
                continue
                
            # Extract directory name from link
            link_cell = cells[1]
            link = link_cell.find('a')
            if not link:
                continue
                
            directory_name = link.text.strip()
            
            # Check if this matches YYYY-MM pattern
            match = self.period_pattern.match(directory_name)
            if not match:
                continue
                
            year = int(match.group(1))
            month = int(match.group(2))
            
            # Validate month range
            if not (1 <= month <= 12):
                continue
            
            # Extract last modified date
            modified_cell = cells[2]
            last_modified = modified_cell.text.strip()
            
            # Build full URL
            href = link.get('href', '')
            full_url = urljoin(self.base_url, href)
            
            period = DataPeriod(
                year=year,
                month=month,
                directory_name=directory_name,
                last_modified=last_modified,
                url=full_url
            )
            
            periods.append(period)
        
        # Sort by year, month (newest first)
        periods.sort(key=lambda p: (p.year, p.month), reverse=True)
        
        return periods
    
    def discover_available_periods(self) -> List[DataPeriod]:
        """
        Discover all available data periods from Federal Revenue.
        
        Returns:
            List of DataPeriod objects, sorted by date (newest first)
        """
        html_content = self.fetch_directory_listing()
        return self.parse_directory_listing(html_content)
    
    def get_latest_period(self) -> Optional[DataPeriod]:
        """
        Get the most recent available data period.
        
        Returns:
            Latest DataPeriod or None if no periods found
        """
        periods = self.discover_available_periods()
        return periods[0] if periods else None
    
    def find_period(self, year: int, month: int) -> Optional[DataPeriod]:
        """
        Find a specific data period.
        
        Args:
            year: Target year
            month: Target month
            
        Returns:
            DataPeriod if found, None otherwise
        """
        periods = self.discover_available_periods()
        
        for period in periods:
            if period.year == year and period.month == month:
                return period
        
        return None
    
    def get_periods_since(self, year: int, month: int) -> List[DataPeriod]:
        """
        Get all periods since (and including) a specific year/month.
        
        Args:
            year: Starting year
            month: Starting month
            
        Returns:
            List of DataPeriod objects from the specified date onwards
        """
        periods = self.discover_available_periods()
        target_date = (year, month)
        
        return [
            period for period in periods
            if (period.year, period.month) >= target_date
        ]
    
    def get_periods_for_year(self, year: int) -> List[DataPeriod]:
        """
        Get all available periods for a specific year.
        
        Args:
            year: Target year
            
        Returns:
            List of DataPeriod objects for the specified year
        """
        periods = self.discover_available_periods()
        return [period for period in periods if period.year == year]
    
    def get_summary(self, periods: List[DataPeriod]) -> Dict[str, any]:
        """
        Generate a summary of discovered periods.
        
        Args:
            periods: List of DataPeriod objects
            
        Returns:
            Dictionary with summary statistics
        """
        if not periods:
            return {
                'total_periods': 0,
                'date_range': None,
                'latest_period': None,
                'years_covered': [],
                'periods': []
            }
        
        latest = periods[0]  # Already sorted newest first
        oldest = periods[-1]
        
        years_covered = sorted(list(set(p.year for p in periods)))
        
        return {
            'total_periods': len(periods),
            'date_range': f"{oldest.period_str} to {latest.period_str}",
            'latest_period': latest.period_str,
            'years_covered': years_covered,
            'periods': [p.period_str for p in periods[:10]]  # Show first 10
        }
    
    def validate_period_availability(self, year: int, month: int) -> bool:
        """
        Check if a specific period is available.
        
        Args:
            year: Target year
            month: Target month
            
        Returns:
            True if period is available, False otherwise
        """
        return self.find_period(year, month) is not None
    
    def scrape_period_files(self, year: int, month: int) -> List[PeriodFileInfo]:
        """
        Scrape files from a specific period directory.
        
        Args:
            year: Target year
            month: Target month
            
        Returns:
            List of PeriodFileInfo objects for files in the period
            
        Raises:
            requests.RequestException: If request fails
            ValueError: If period is not available
        """
        # Find the period first
        period = self.find_period(year, month)
        if not period:
            raise ValueError(f"Period {year:04d}-{month:02d} is not available")
        
        return self.scrape_files_from_url(period.url)
    
    def scrape_files_from_url(self, period_url: str) -> List[PeriodFileInfo]:
        """
        Scrape files from a specific period URL.
        
        Args:
            period_url: URL of the period directory
            
        Returns:
            List of PeriodFileInfo objects for files in the period
            
        Raises:
            requests.RequestException: If request fails
        """
        from src.utils.misc import convert_to_bytes
        import pytz
        
        logger.info(f"Scraping files from period URL: {period_url}")
        
        # Set up HTTP session with retries
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        
        try:
            response = session.get(
                period_url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; CNPJ-Scraper/1.0)"},
                timeout=30,
            )
            response.raise_for_status()
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch period URL: {period_url}, error: {e}")
            raise
        
        # Parse HTML content
        soup = BeautifulSoup(response.content, "lxml")
        table_rows = soup.find_all("tr")
        
        files_info = []
        for row in table_rows:
            filename_cell = row.find("a")
            date_cell = row.find("td", text=lambda t: t and re.search(r"\d{4}-\d{2}-\d{2}", t))
            size_cell = row.find(
                "td",
                text=lambda t: t and any(t.endswith(s) for s in ["K","M","G","T","P","E","Z","Y"])
            )
            
            if filename_cell and date_cell and size_cell:
                filename = filename_cell.text.strip()
                if filename.endswith(".zip"):
                    # Parse date
                    updated_at = self._parse_file_date(date_cell.text.strip(), filename)
                    if not updated_at:
                        continue
                    
                    # Build download URL
                    href = filename_cell.get('href', '')
                    download_url = urljoin(period_url, href)
                    
                    file_info = PeriodFileInfo(
                        filename=filename,
                        updated_at=updated_at,
                        file_size=convert_to_bytes(size_cell.text.strip()),
                        download_url=download_url
                    )
                    files_info.append(file_info)
        
        logger.info(f"Found {len(files_info)} files in period directory")
        return files_info
    
    def _parse_file_date(self, date_str: str, filename: str) -> Optional[datetime]:
        """
        Parse file date from HTML table.
        
        Args:
            date_str: Date string from HTML
            filename: Filename for error logging
            
        Returns:
            Parsed datetime or None if parsing fails
        """
        try:
            updated_at = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            # Use Brazil timezone as default, or try to get from config
            timezone_str = 'America/Sao_Paulo'
            try:
                timezone_str = self.config.pipeline.data_source.timezone    
            except AttributeError:
                pass  # Use default timezone
                
            import pytz
            tz = pytz.timezone(timezone_str)
            return (
                tz.localize(updated_at)
                .replace(hour=0, minute=0, second=0, microsecond=0)
            )
        except ValueError:
            logger.error(f"Error parsing date for file: {filename}")
            return None
    
    def close(self):
        """Close the HTTP session."""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Factory function for integration with existing service architecture
def create_discovery_service(config: AppConfig) -> FederalRevenueDiscoveryService:
    """
    Factory function to create discovery service.
    
    Args:
        config: Application configuration
        
    Returns:
        Initialized FederalRevenueDiscoveryService
    """
    return FederalRevenueDiscoveryService(config)