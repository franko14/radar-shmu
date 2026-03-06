"""
iMeteo Radar - Multi-source radar data processor

A modern, high-performance radar data processing system for DWD, SHMU, CHMI, ARSO, OMSZ, and IMGW weather radar data.
Supports parallel downloads, fast interpolation, and optimized PNG exports.
"""

__version__ = "2.9.2"
__author__ = "Radar Processing Team"

from .processing.exporter import ExportConfig, MultiFormatExporter
from .sources.arso import ARSORadarSource
from .sources.chmi import CHMIRadarSource
from .sources.dwd import DWDRadarSource
from .sources.imgw import IMGWRadarSource
from .sources.omsz import OMSZRadarSource
from .sources.shmu import SHMURadarSource

__all__ = [
    "SHMURadarSource",
    "DWDRadarSource",
    "CHMIRadarSource",
    "ARSORadarSource",
    "OMSZRadarSource",
    "IMGWRadarSource",
    "MultiFormatExporter",
    "ExportConfig",
]
