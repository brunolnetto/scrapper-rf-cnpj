"""
Example usage of the Federal Revenue Discovery Service.

This demonstrates the discovery service integrated into the ETL pipeline
following the established lazy initialization pattern.
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.setup.config import get_config
from src.core.etl import ReceitaCNPJPipeline


def main():
    """
    Demonstrate discovery service usage through the ETL pipeline.
    
    The discovery service is now integrated as a lazy property in the
    ReceitaCNPJPipeline class, following the same pattern as audit_service
    and data_loader.
    """
    
    print("🔍 Federal Revenue Data Discovery via ETL Pipeline")
    print("=" * 60)
    
    try:
        # Initialize pipeline (discovery service will be lazy-loaded)
        config = get_config()
        pipeline = ReceitaCNPJPipeline(config)
        
        print(f"📋 Pipeline initialized successfully")
        print(f"🔧 Discovery service available as: pipeline.discovery_service")
        
        # Access discovery service (triggers lazy initialization)
        print(f"\n📅 Discovering available data periods...")
        discovery = pipeline.discovery_service
        print(f"   Using service: {type(discovery).__name__}")
        print(f"   Base URL: {discovery.config.pipeline.data_source.base_url}")
        
        # Attempt to discover periods
        periods = discovery.discover_available_periods()
        
        if not periods:
            print(f"❌ No periods discovered!")
            return
        
        print(f"✅ Successfully discovered {len(periods)} periods")
        
        # Show discovered periods (first 10)
        print(f"\n📊 Available periods (showing first 10):")
        for i, period in enumerate(periods[:10], 1):
            age_days = (datetime.now() - datetime.strptime(period.last_modified.split()[0], "%Y-%m-%d")).days
            age_str = f"({age_days} days ago)" if age_days > 0 else "(today)"
            print(f"   {i:2}. {period.period_str} - {period.last_modified} {age_str}")
        
        if len(periods) > 10:
            print(f"   ... and {len(periods) - 10} more periods")
        
        # Show summary
        summary = discovery.get_summary(periods)
        print(f"\n📈 Summary:")
        print(f"   Total periods: {summary['total_periods']}")
        print(f"   Date range: {summary['date_range']}")
        print(f"   Years covered: {', '.join(map(str, summary['years_covered']))}")
        
        # Show latest period details
        latest = periods[0]
        print(f"\n🎯 Latest Period:")
        print(f"   Period: {latest.period_str}")
        print(f"   URL: {latest.url}")
        print(f"   Modified: {latest.last_modified}")
        
        # Demonstrate how this integrates with ETL workflow
        print(f"\n🚀 ETL Integration Example:")
        print(f"   # Discover latest period")
        print(f"   pipeline = ReceitaCNPJPipeline(config)")
        print(f"   periods = pipeline.discovery_service.discover_available_periods()")
        print(f"   latest = periods[0]")
        print(f"   ")
        print(f"   # Configure and run ETL for discovered period")
        print(f"   # orchestrator.run(year={latest.year}, month={latest.month})")
        
        print(f"\n✅ Discovery service working correctly through pipeline!")
        
    except Exception as e:
        print(f"❌ Failed to discover periods: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

from datetime import datetime

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.setup.config import get_config
from src.core.services.discovery.service import FederalRevenueDiscoveryService


def main():
    """Example usage of the discovery service."""
    
    # Get configuration
    config = get_config()
    
    print("🔍 Federal Revenue Data Discovery")
    print("=" * 50)
    
    # Create and use discovery service
    with FederalRevenueDiscoveryService(config) as discovery:
        
        # Discover all available periods
        print("\n📅 Discovering available data periods...")
        try:
            periods = discovery.discover_available_periods()
        except Exception as e:
            print(f"❌ Failed to discover periods: {e}")
            return
        
        if not periods:
            print("❌ No data periods found!")
            return
        
        # Show summary
        summary = discovery.get_summary(periods)
        print(f"✅ Found {summary['total_periods']} available periods")
        print(f"📊 Date range: {summary['date_range']}")
        print(f"🆕 Latest period: {summary['latest_period']}")
        print(f"📈 Years covered: {summary['years_covered']}")
        
        # Show latest few periods
        print(f"\n📋 Latest 5 periods:")
        for i, period in enumerate(periods[:5], 1):
            status = "🆕 LATEST" if i == 1 else ""
            print(f"   {i:2}. {period.period_str} - {period.last_modified} {status}")
        
        # Get latest period details
        print(f"\n🔍 Latest period details:")
        latest = discovery.get_latest_period()
        if latest:
            print(f"   Period: {latest.period_str}")
            print(f"   Directory: {latest.directory_name}")
            print(f"   URL: {latest.url}")
            print(f"   Modified: {latest.last_modified}")
            print(f"   Is current month: {latest.is_current_month}")
        
        # Find specific period
        print(f"\n🎯 Searching for specific periods:")
        
        # Try to find current year data
        current_year = datetime.now().year
        current_periods = discovery.get_periods_for_year(current_year)
        if current_periods:
            print(f"   Found {len(current_periods)} periods for {current_year}")
            for period in current_periods:
                print(f"     - {period.period_str}")
        
        # Try to find a specific period (e.g., 2024-12)
        specific = discovery.find_period(2024, 12)
        if specific:
            print(f"   ✅ Found 2024-12: {specific.url}")
        else:
            print(f"   ❌ Period 2024-12 not found")
        
        # Get recent periods
        print(f"\n📈 Recent periods (since 2024-01):")
        recent = discovery.get_periods_since(2024, 1)
        for period in recent[:5]:  # Show first 5
            print(f"   - {period.period_str}")
        
        # Test validation
        print(f"\n✅ Validation tests:")
        test_periods = [(2025, 9), (2024, 12), (2023, 1)]
        for year, month in test_periods:
            available = discovery.validate_period_availability(year, month)
            status = "✅" if available else "❌"
            print(f"   {status} {year}-{month:02d}: {'Available' if available else 'Not found'}")
        
        print(f"\n🎉 Discovery complete!")


if __name__ == "__main__":
    main()