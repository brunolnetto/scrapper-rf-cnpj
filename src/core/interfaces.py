from typing import Protocol, Optional, Any


class Pipeline(Protocol):
    """Pipeline interface for orchestrator agnosticism."""
    
    def run(self, **kwargs) -> Optional[Any]:
        """Execute the pipeline with given parameters."""
        ...
    
    def validate_config(self) -> bool:
        """Validate pipeline configuration."""
        ...
    
    def get_name(self) -> str:
        """Return pipeline name for logging."""
        ...


class OrchestrationStrategy(Protocol):
    """Strategy interface for different orchestration patterns."""
    
    async def execute(self, pipeline: Pipeline, config_service, **kwargs) -> Optional[Any]:
        """Execute the pipeline using this orchestration strategy."""
        ...
    
    def get_name(self) -> str:
        """Return strategy name for logging."""
        ...
    
    def validate_parameters(self, **kwargs) -> bool:
        """Validate that required parameters are provided."""
        ...