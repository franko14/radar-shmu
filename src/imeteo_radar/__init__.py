"""
Radar SHMU - Multi-source radar data processor

A modern, high-performance radar data processing system for DWD, SHMU, CHMI, ARSO, and OMSZ weather radar data.
Supports parallel downloads, fast interpolation, and optimized PNG exports.
"""

__version__ = "1.4.0"
__author__ = "Radar Processing Team"

from .processing.animator import RadarAnimator
from .processing.exporter import PNGExporter
from .processing.merger import RadarMerger
from .sources.arso import ARSORadarSource
from .sources.chmi import CHMIRadarSource
from .sources.dwd import DWDRadarSource
from .sources.imgw import IMGWRadarSource
from .sources.omsz import OMSZRadarSource

# Main exports
from .sources.shmu import SHMURadarSource

__all__ = [
    "SHMURadarSource",
    "DWDRadarSource",
    "CHMIRadarSource",
    "ARSORadarSource",
    "OMSZRadarSource",
    "IMGWRadarSource",
    "RadarMerger",
    "PNGExporter",
    "RadarAnimator",
]
