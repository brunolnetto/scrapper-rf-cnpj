import time
from datetime import datetime

from ..setup.logging import logger
from .interfaces import Pipeline, OrchestrationStrategy

class GenericOrchestrator:
    def __init__(self, pipeline: Pipeline, strategy: OrchestrationStrategy, config_service):
        self.pipeline = pipeline
        self.strategy = strategy
        self.config_service = config_service

    def run(self, **kwargs):
        """
        Run the pipeline using the configured strategy.
        
        Args:
            **kwargs: Parameters to pass to the strategy
        """
        start_time = time.time()
        
        logger.info(f"Orchestrator start: {datetime.now():%Y-%m-%d %H:%M:%S}")
        logger.info(f"Pipeline: {self.pipeline.get_name()}")
        logger.info(f"Strategy: {self.strategy.get_name()}")
        
        # Validate pipeline configuration
        if not self.pipeline.validate_config():
            logger.error(f"[ERROR] Invalid configuration for pipeline: {self.pipeline.get_name()}")
            return None
        
        # Validate strategy parameters
        if not self.strategy.validate_parameters(**kwargs):
            logger.error(f"[ERROR] Invalid parameters for strategy: {self.strategy.get_name()}")
            return None
        
        try:
            # Execute using strategy
            result = self.strategy.execute(self.pipeline, self.config_service, **kwargs)
            
            logger.info(f"[SUCCESS] {self.strategy.get_name()} strategy completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"[ERROR] {self.strategy.get_name()} strategy failed: {e}")
            raise
        finally:
            # Calculate and log execution time
            execution_time = time.time() - start_time
            logger.info(f"[METRICS] Total execution time: {execution_time:.2f} seconds")

