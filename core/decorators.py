import functools
import logging
import time
from typing import Any, Callable, Type
from core.base import BaseJob


def register_feature(name: str):
    """Decorator to register a feature class with the registry."""

    def decorator(feature_class: Type[BaseJob]):
        from core.registry import get_registry
        registry = get_registry()
        registry.register(name, feature_class)
        return feature_class

    return decorator


def timing_decorator(func: Callable) -> Callable:
    """Decorator to measure execution time of functions."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        logger = logging.getLogger(func.__module__)
        logger.info(f"{func.__name__} executed in {end_time - start_time:.4f} seconds")

        return result

    return wrapper


def error_handler(func: Callable) -> Callable:
    """Decorator to handle errors gracefully."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger(func.__module__)
            logger.error(f"Error in {func.__name__}: {e}")
            raise

    return wrapper


def validate_input(func: Callable) -> Callable:
    """Decorator to validate input parameters."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        # Basic validation - can be extended based on specific needs
        if 'path' in kwargs and not kwargs['path']:
            raise ValueError("Path parameter cannot be empty")

        if 'num_workers' in kwargs and kwargs['num_workers'] < 1:
            raise ValueError("Number of workers must be at least 1")

        return func(*args, **kwargs)

    return wrapper
