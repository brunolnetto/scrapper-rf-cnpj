"""
Integration demo showing discovery service working with the ETL pipeline.

This demonstrates how the discovery service integrates with the existing 
ReceitaCNPJPipeline and orchestrator patterns using the lazy property approach.
"""

import sys
import os
from datetime import datetime
from typing import List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.setup.config import get_config
from src.core.etl import ReceitaCNPJPipeline
from src.core.services.discovery.service import DataPeriod
from src.core.orchestrator import PipelineOrchestrator
from src.core.strategies import StrategyFactory


# Mock periods for demonstration (since network access fails)
MOCK_PERIODS = [
    DataPeriod(
        year=2025,
        month=9,
        directory_name="2025-09",
        last_modified="2025-09-14 16:38",
        url="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/"
    ),
    DataPeriod(
        year=2024,
        month=12,
        directory_name="2024-12", 
        last_modified="2024-12-15 10:22",
        url="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2024-12/"
    ),
    DataPeriod(
        year=2024,
        month=11,
        directory_name="2024-11",
        last_modified="2024-11-20 14:15",
        url="https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2024-11/"
    )
]


class EnhancedOrchestrator:
    """
    Enhanced orchestrator that can discover and process the latest available data periods.
    
    This demonstrates how the discovery service integrates into the existing 
    ETL pipeline architecture using the lazy property pattern.
    """
    
    def __init__(self):
        self.config = get_config()
        self.pipeline = ReceitaCNPJPipeline(self.config)
        
    def discover_periods(self) -> List[DataPeriod]:
        """Discover available periods using pipeline's discovery service."""
        try:
            return self.pipeline.discovery_service.discover_available_periods()
        except Exception as e:
            print(f"⚠️  Network discovery failed: {e}")
            print(f"🎭 Using mock data for demonstration")
            return MOCK_PERIODS
    
    def get_latest_period(self) -> Optional[DataPeriod]:
        """Get the most recent available data period."""
        periods = self.discover_periods()
        return periods[0] if periods else None
    
    def get_period_for_date(self, year: int, month: int) -> Optional[DataPeriod]:
        """Find a specific period by year and month."""
        periods = self.discover_periods()
        for period in periods:
            if period.year == year and period.month == month:
                return period
        return None
    
    def run_latest_etl(self, strategy_flags: dict = None):
        """Run ETL for the latest available period."""
        
        print(f"🚀 Enhanced ETL Pipeline - Latest Data Processing")
        print(f"=" * 60)
        
        # Discover latest period
        latest = self.get_latest_period()
        if not latest:
            print(f"❌ No data periods available!")
            return False
            
        print(f"📅 Latest period discovered: {latest.period_str}")
        print(f"📍 URL: {latest.url}")
        print(f"🕒 Last modified: {latest.last_modified}")
        
        # Configure ETL for this period
        return self.run_etl_for_period(latest, strategy_flags)
    
    def run_etl_for_period(self, period: DataPeriod, strategy_flags: dict = None):
        """Run ETL for a specific period."""
        
        strategy_flags = strategy_flags or {
            'download': True,
            'convert': True, 
            'load': True
        }
        
        print(f"\n🔧 ETL Configuration:")
        print(f"   Year: {period.year}")
        print(f"   Month: {period.month}")
        print(f"   Strategy: {strategy_flags}")
        
        try:
            # Create pipeline and strategy (pipeline already exists)
            strategy = StrategyFactory.create_strategy(**strategy_flags)
            config_service = self.config
            
            # Create standard orchestrator 
            orchestrator = PipelineOrchestrator(self.pipeline, strategy, config_service)
            
            # Here we would run the ETL
            print(f"\n▶️  Starting ETL for period {period.period_str}...")
            print(f"   (Simulated - actual execution would call orchestrator.run())")
            
            # Simulate processing steps
            steps = []
            if strategy_flags.get('download'):
                steps.append("📥 Download ZIP files from Federal Revenue")
            if strategy_flags.get('convert'): 
                steps.append("🔄 Convert CSV to Parquet format")
            if strategy_flags.get('load'):
                steps.append("📊 Load data into PostgreSQL")
                
            for i, step in enumerate(steps, 1):
                print(f"   {i}. {step}")
            
            print(f"✅ ETL simulation completed for {period.period_str}")
            return True
            
        except Exception as e:
            print(f"❌ ETL failed: {e}")
            return False
    
    def show_available_periods(self):
        """Display all available periods."""
        
        print(f"📋 Available Data Periods")
        print(f"=" * 40)
        
        periods = self.discover_periods()
        
        if not periods:
            print(f"❌ No periods found")
            return
            
        for i, period in enumerate(periods, 1):
            age_days = (datetime.now() - datetime.strptime(period.last_modified.split()[0], "%Y-%m-%d")).days
            age_str = f"({age_days} days ago)" if age_days > 0 else "(today)"
            
            print(f"   {i:2}. {period.period_str} - Modified: {period.last_modified} {age_str}")
        
        # Show summary
        summary = self.pipeline.discovery_service.get_summary(periods)
        print(f"\n📊 Summary:")
        print(f"   Total periods: {summary['total_periods']}")
        print(f"   Date range: {summary['date_range']}")
        print(f"   Years covered: {', '.join(map(str, summary['years_covered']))}")


def main():
    """Main demonstration function."""
    
    try:
        orchestrator = EnhancedOrchestrator()
        
        # Show available periods
        orchestrator.show_available_periods()
        
        print(f"\n")
        
        # Run ETL for latest period
        orchestrator.run_latest_etl({
            'download': True,
            'convert': True,
            'load': False  # Skip loading for demo
        })
        
        print(f"\n")
        
        # Example: Run ETL for specific period
        specific_period = orchestrator.get_period_for_date(2024, 12)
        if specific_period:
            print(f"🎯 Running ETL for specific period: {specific_period.period_str}")
            orchestrator.run_etl_for_period(specific_period, {
                'download': False,
                'convert': True,
                'load': True
            })
        
        print(f"\n🎉 Discovery service integration demonstration complete!")
        print(f"💡 This shows how period discovery can enhance the ETL pipeline automation.")
        
    except Exception as e:
        print(f"❌ Error in integration demo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()