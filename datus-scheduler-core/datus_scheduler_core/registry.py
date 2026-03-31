# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

import importlib
import logging
import threading
from typing import ClassVar, Dict, Optional, Type

from datus_scheduler_core.base import BaseSchedulerAdapter
from datus_scheduler_core.exceptions import SchedulerException

logger = logging.getLogger(__name__)


class SchedulerAdapterMetadata:
    """Metadata about a registered scheduler adapter."""

    def __init__(
        self,
        platform: str,
        adapter_class: Type[BaseSchedulerAdapter],
        config_class: Optional[Type] = None,
        display_name: Optional[str] = None,
    ) -> None:
        self.platform = platform
        self.adapter_class = adapter_class
        self.config_class = config_class
        self.display_name = display_name or platform.capitalize()


class SchedulerAdapterRegistry:
    """Central registry for Datus scheduler adapters.

    Adapter discovery uses two complementary mechanisms — the same dual-mode
    pattern used by ``ConnectorRegistry`` in ``datus-db-core``:

    1. **entry_points** (``datus.schedulers`` group): auto-discovers every
       installed ``datus-{platform}`` package.
    2. **Dynamic import** (``datus_{platform}``): on-demand import triggered
       by ``create_adapter`` when the requested platform is not yet registered.

    Adapters self-register by calling ``registry.register(...)`` inside their
    ``register()`` function, which is invoked by both mechanisms.
    """

    _adapters: ClassVar[Dict[str, Type[BaseSchedulerAdapter]]] = {}
    _metadata: ClassVar[Dict[str, SchedulerAdapterMetadata]] = {}
    _initialized: ClassVar[bool] = False
    _lock: ClassVar[threading.RLock] = threading.RLock()

    @classmethod
    def register(
        cls,
        platform: str,
        adapter_class: Type[BaseSchedulerAdapter],
        config_class: Optional[Type] = None,
        display_name: Optional[str] = None,
    ) -> None:
        """Register a scheduler adapter."""
        key = platform.strip().lower()
        with cls._lock:
            cls._adapters[key] = adapter_class
            cls._metadata[key] = SchedulerAdapterMetadata(
                platform=key,
                adapter_class=adapter_class,
                config_class=config_class,
                display_name=display_name or platform.capitalize(),
            )
        logger.debug("Registered scheduler adapter: %s -> %s", platform, adapter_class.__name__)

    @classmethod
    def create_adapter(cls, platform: str, config) -> BaseSchedulerAdapter:
        """Instantiate and return an adapter for the given platform.

        Tries entry_points discovery and dynamic import before raising.
        """
        key = platform.strip().lower()
        cls.discover_adapters()
        if key not in cls._adapters:
            cls._try_load_adapter(key)
        if key not in cls._adapters:
            available = list(cls._adapters.keys())
            raise SchedulerException(
                f"Scheduler adapter '{platform}' not found. "
                f"Available: {available}. "
                f"Install the missing adapter: pip install datus-{key}"
            )
        return cls._adapters[key](config)

    @classmethod
    def _try_load_adapter(cls, platform: str) -> None:
        """Attempt to dynamically import ``datus_{platform}`` and call ``register()``."""
        module_name = f"datus_scheduler_{platform}"
        try:
            module = importlib.import_module(module_name)
            if hasattr(module, "register"):
                module.register()
                logger.info("Dynamically loaded scheduler adapter: %s", platform)
        except ImportError as exc:
            if getattr(exc, "name", None) == module_name:
                logger.debug("No scheduler adapter package found for: %s", platform)
                return
            raise
        except Exception as exc:
            raise SchedulerException(
                f"Failed to load scheduler adapter '{platform}': {exc}"
            ) from exc

    @classmethod
    def discover_adapters(cls) -> None:
        """Discover all installed adapters via the ``datus.schedulers`` entry_points group."""
        if cls._initialized:
            return
        with cls._lock:
            if cls._initialized:
                return
            try:
                from importlib.metadata import entry_points

                try:
                    eps = entry_points(group="datus.schedulers")
                except TypeError:
                    all_eps = entry_points()
                    eps = all_eps.get("datus.schedulers", [])

                for ep in eps:
                    try:
                        register_func = ep.load()
                        register_func()
                        logger.info("Discovered scheduler adapter: %s", ep.name)
                    except Exception as exc:
                        logger.warning("Failed to load scheduler adapter %s: %s", ep.name, exc)
            except Exception as exc:
                logger.warning("Entry points discovery failed: %s", exc)
            finally:
                cls._initialized = True

    @classmethod
    def list_adapters(cls) -> Dict[str, Type[BaseSchedulerAdapter]]:
        """Return a snapshot of all currently registered adapters."""
        return cls._adapters.copy()

    @classmethod
    def is_registered(cls, platform: str) -> bool:
        return platform.strip().lower() in cls._adapters

    @classmethod
    def get_metadata(cls, platform: str) -> Optional[SchedulerAdapterMetadata]:
        cls.discover_adapters()
        return cls._metadata.get(platform.strip().lower())

    @classmethod
    def reset(cls) -> None:
        """Reset registry state (for testing only)."""
        with cls._lock:
            cls._adapters.clear()
            cls._metadata.clear()
            cls._initialized = False


# Module-level singleton
scheduler_registry = SchedulerAdapterRegistry()
