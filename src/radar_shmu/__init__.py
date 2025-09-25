"""
Radar SHMU - Multi-source radar data processor

A modern, high-performance radar data processing system for SHMU and DWD weather radar data.
Supports parallel downloads, fast interpolation, and optimized PNG exports.
"""

__version__ = "1.0.0"
__author__ = "Radar Processing Team"

# Main exports
from .sources.shmu import SHMURadarSource
from .sources.dwd import DWDRadarSource
from .processing.merger import RadarMerger
from .processing.exporter import PNGExporter
from .processing.animator import RadarAnimator

__all__ = [
    "SHMURadarSource", 
    "DWDRadarSource",
    "RadarMerger",
    "PNGExporter",
    "RadarAnimator",
]