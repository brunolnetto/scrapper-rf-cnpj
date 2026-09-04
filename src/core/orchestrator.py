import time
import uuid
from datetime import datetime

from ..setup.logging import logger
from .interfaces import Pipeline, OrchestrationStrategy
from sqldim.lineage import (
    ConsoleLineageEmitter,
    LineageEvent,
    RunState,
)
from sqldim.lineage.events import DatasetRef
from sqldim.notifications import NotificationEvent, Severity
from sqldim.medallion import Layer
from .services.notifications.router import make_notification_router

class PipelineOrchestrator:
    def __init__(self, pipeline: Pipeline, strategy: OrchestrationStrategy, config_service):
        self.pipeline = pipeline
        self.strategy = strategy
        self.config_service = config_service
        self._router = make_notification_router(config_service)

    def run(self, **kwargs):
        """
        Run the pipeline using the configured strategy.
        
        Args:
            **kwargs: Parameters to pass to the strategy
        """
        start_time = time.perf_counter()
        
        logger.info(f"Orchestrator start: {datetime.now():%Y-%m-%d %H:%M:%S}")
        logger.info(f"Pipeline: {self.pipeline.get_name()}")
        logger.info(f"Strategy: {self.strategy.get_name()}")
        
        # Handle temporal configuration (year/month)
        year = kwargs.get('year')
        month = kwargs.get('month')
        if year is not None or month is not None:
            # Set temporal config directly on the config object
            if year is not None:
                self.config_service._year = year
                # Also update pipeline config if it exists
                self.config_service.pipeline.year = year
            if month is not None:
                self.config_service._month = month

                # Also update pipeline config if it exists
                self.config_service.pipeline.month = month
            
            current_year = getattr(self.config_service, '_year', year)
            current_month = getattr(self.config_service, '_month', month)
            logger.info(f"Configured temporal settings: year={current_year}, month={current_month}")
        
        # Validate pipeline configuration
        if not self.pipeline.validate_config():
            logger.error(f"[ERROR] Invalid configuration for pipeline: {self.pipeline.get_name()}")
            return None
        
        # Validate strategy parameters
        if not self.strategy.validate_parameters(**kwargs):
            logger.error(f"[ERROR] Invalid parameters for strategy: {self.strategy.get_name()}")
            return None

        # --- Lineage ---
        emitter = ConsoleLineageEmitter()
        run_id = uuid.uuid4().hex
        _lineage_inputs  = [DatasetRef(namespace="scrapper-rf-cnpj.bronze", name="cnpj_raw_files")]
        _lineage_outputs = [DatasetRef(namespace="scrapper-rf-cnpj.gold",   name="cnpj_dimensions")]
        _lineage_facets: dict = {}
        if year is not None:
            _lineage_facets["year"] = year
        if month is not None:
            _lineage_facets["month"] = month
        emitter.emit(LineageEvent(
            run_id=run_id,
            job_name=self.pipeline.get_name(),
            namespace="scrapper-rf-cnpj",
            state=RunState.START,
            inputs=_lineage_inputs,
            outputs=_lineage_outputs,
            facets=_lineage_facets,
        ))

        try:
            # Execute using strategy
            result = self.strategy.execute(self.pipeline, self.config_service, **kwargs)
            
            emitter.emit(LineageEvent(
                run_id=run_id,
                job_name=self.pipeline.get_name(),
                namespace="scrapper-rf-cnpj",
                state=RunState.COMPLETE,
                inputs=_lineage_inputs,
                outputs=_lineage_outputs,
                facets=_lineage_facets,
            ))
            logger.info(f"[SUCCESS] {self.strategy.get_name()} strategy completed successfully")
            return result
            
        except Exception as e:
            emitter.emit(LineageEvent(
                run_id=run_id,
                job_name=self.pipeline.get_name(),
                namespace="scrapper-rf-cnpj",
                state=RunState.FAIL,
                inputs=_lineage_inputs,
                outputs=_lineage_outputs,
                facets={**_lineage_facets, "error": str(e)},
            ))
            logger.error(f"[ERROR] {self.strategy.get_name()} strategy failed: {e}")

            # P1 alert — pipeline crash requires immediate on-call response.
            try:
                self._router.route(NotificationEvent(
                    event_type="pipeline_crash",
                    severity=Severity.P1,
                    layer=Layer.GOLD,
                    details={
                        "pipeline": self.pipeline.get_name(),
                        "strategy": self.strategy.get_name(),
                        "error": str(e)[:500],
                    },
                ))
            except Exception as notif_err:
                logger.debug(f"[Notifications] P1 dispatch failed: {notif_err}")

            raise
        finally:
            # Calculate and log execution time
            execution_time = time.perf_counter() - start_time
            logger.info(f"[METRICS] Total execution time: {execution_time:.2f} seconds")

            import os
            os._exit(0)

