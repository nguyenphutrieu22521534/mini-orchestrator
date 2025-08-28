import logging
from typing import Dict, Optional, Type
from core.base import BaseJob

class FeatureRegistry:
    """Registry for managing features in the orchestrator."""

    def __init__(self):
        self._features: Dict[str, Type[BaseJob]] = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def register(self, name: str, feature_class: Type[BaseJob]) -> None:
        """Register a feature class with the given name."""
        self._features[name] = feature_class
        self.logger.info(f"Registered feature: {name}")

    def get_feature(self, name: str) -> Optional[BaseJob]:
        """Get a feature instance by name."""
        if name not in self._features:
            self.logger.error(f"Feature '{name}' not found")
            return None

        feature_class = self._features[name]
        return feature_class()

    def list_features(self) -> list:
        """List all registered features."""
        return list(self._features.keys())

    def has_feature(self, name: str) -> bool:
        """Check if a feature is registered."""
        return name in self._features


# Global registry instance
_registry = FeatureRegistry()


def get_registry() -> FeatureRegistry:
    """Get the global feature registry instance."""
    return _registry


def register_feature(name: str):
    """Decorator to register a feature class."""

    def decorator(feature_class: Type[BaseJob]):
        _registry.register(name, feature_class)
        return feature_class

    return decorator
