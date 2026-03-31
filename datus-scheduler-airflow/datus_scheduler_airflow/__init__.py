# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

from datus_scheduler_airflow.adapter import AirflowSchedulerAdapter
from datus_scheduler_core.config import AirflowConfig

__version__ = "0.1.0"
__all__ = ["AirflowSchedulerAdapter", "AirflowConfig", "register"]


def register() -> None:
    """Register the Airflow adapter with the global ``scheduler_registry``.

    This function is called:
    * Automatically via the ``datus.schedulers`` entry_point when the package
      is installed and ``SchedulerAdapterRegistry.discover_adapters()`` runs.
    * Explicitly via ``importlib.import_module("datus_scheduler_airflow").register()``
      when the registry attempts dynamic loading.
    """
    from datus_scheduler_core import scheduler_registry

    scheduler_registry.register(
        platform="airflow",
        adapter_class=AirflowSchedulerAdapter,
        config_class=AirflowConfig,
        display_name="Apache Airflow",
    )
