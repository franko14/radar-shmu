#!/usr/bin/env python3
"""
Centralized source registry for all radar data sources.

This module provides a single source of truth for source configurations,
eliminating duplication across cli.py, cli_composite.py, and spaces_uploader.py.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..core.base import RadarSource


# Central registry of all radar sources
SOURCE_REGISTRY: dict[str, dict[str, Any]] = {
    "dwd": {
        "class_name": "DWDRadarSource",
        "module": "imeteo_radar.sources.dwd",
        "product": "dmax",
        "country": "germany",
        "folder": "germany",
        "description": "German Weather Service (DWD)",
    },
    "shmu": {
        "class_name": "SHMURadarSource",
        "module": "imeteo_radar.sources.shmu",
        "product": "zmax",
        "country": "slovakia",
        "folder": "slovakia",
        "description": "Slovak Hydrometeorological Institute (SHMU)",
    },
    "chmi": {
        "class_name": "CHMIRadarSource",
        "module": "imeteo_radar.sources.chmi",
        "product": "maxz",
        "country": "czechia",
        "folder": "czechia",
        "description": "Czech Hydrometeorological Institute (CHMI)",
    },
    "arso": {
        "class_name": "ARSORadarSource",
        "module": "imeteo_radar.sources.arso",
        "product": "zm",
        "country": "slovenia",
        "folder": "slovenia",
        "description": "Slovenian Environment Agency (ARSO)",
    },
    "omsz": {
        "class_name": "OMSZRadarSource",
        "module": "imeteo_radar.sources.omsz",
        "product": "cmax",
        "country": "hungary",
        "folder": "hungary",
        "description": "Hungarian Meteorological Service (OMSZ)",
    },
    "imgw": {
        "class_name": "IMGWRadarSource",
        "module": "imeteo_radar.sources.imgw",
        "product": "cmax",
        "country": "poland",
        "folder": "poland",
        "description": "Polish Institute of Meteorology and Water Management (IMGW)",
    },
}


def get_source_config(source_name: str) -> dict[str, Any] | None:
    """Get configuration for a source by name.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')

    Returns:
        Source configuration dict or None if not found
    """
    return SOURCE_REGISTRY.get(source_name.lower())


def get_source_instance(source_name: str) -> "RadarSource":
    """Create and return a source instance by name.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')

    Returns:
        Instantiated RadarSource subclass

    Raises:
        ValueError: If source_name is not recognized
        ImportError: If the source module cannot be imported
    """
    config = get_source_config(source_name)
    if not config:
        raise ValueError(f"Unknown source: {source_name}")

    # Dynamic import
    import importlib

    module = importlib.import_module(config["module"])
    source_class = getattr(module, config["class_name"])
    return source_class()


def get_folder_for_source(source_name: str) -> str:
    """Get the Spaces folder name for a source.

    Args:
        source_name: Source identifier (e.g., 'dwd', 'shmu')

    Returns:
        Folder name for cloud storage (e.g., 'germany', 'slovakia')
    """
    config = get_source_config(source_name)
    return config["folder"] if config else source_name.lower()


def get_all_source_names() -> list:
    """Get list of all registered source names.

    Returns:
        List of source identifiers
    """
    return list(SOURCE_REGISTRY.keys())
