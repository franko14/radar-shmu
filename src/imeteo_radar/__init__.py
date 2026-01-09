"""
Radar SHMU - Multi-source radar data processor

A modern, high-performance radar data processing system for DWD, SHMU, CHMI, ARSO, and OMSZ weather radar data.
Supports parallel downloads, fast interpolation, and optimized PNG exports.
"""

__version__ = "1.4.0"
__author__ = "Radar Processing Team"

# Main exports
from .sources.shmu import SHMURadarSource
from .sources.dwd import DWDRadarSource
from .sources.chmi import CHMIRadarSource
from .sources.arso import ARSORadarSource
from .sources.omsz import OMSZRadarSource
from .processing.merger import RadarMerger
from .processing.exporter import PNGExporter
from .processing.animator import RadarAnimator

__all__ = [
    "SHMURadarSource",
    "DWDRadarSource",
    "CHMIRadarSource",
    "ARSORadarSource",
    "OMSZRadarSource",
    "RadarMerger",
    "PNGExporter",
    "RadarAnimator",
]