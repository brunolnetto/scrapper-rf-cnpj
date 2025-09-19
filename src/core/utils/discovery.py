from ...setup.logging import logger

def discover_latest_period():
    """
    Discover the latest available period from Federal Revenue.
    
    Returns:
        tuple: (year, month) of latest period, or current date as fallback
    """
    try:
        from ...setup.config import get_config
        from ...core.services.discovery.service import FederalRevenueDiscoveryService
        
        logger.info("Discovering latest available period...")
        config = get_config()
        discovery = FederalRevenueDiscoveryService(config)
        periods = discovery.discover_available_periods()
        
        if periods:
            latest = periods[0]  # Periods are sorted newest first
            logger.info(f"Using latest available period: {latest.period_str}")
            return latest.year, latest.month
            
    except Exception as e:
        logger.warning(f"Failed to discover latest period: {e}")
    
    # Fallback to current date
    from datetime import datetime
    current = datetime.now()
    logger.warning(f"Falling back to current date: {current.year}-{current.month:02d}")
    return current.year, current.month
