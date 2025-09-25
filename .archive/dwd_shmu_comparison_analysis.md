# DWD vs SHMU Radar Products Comparison

## Analysis Overview

This analysis compares DWD (Deutscher Wetterdienst - German Weather Service) radar products with SHMU (Slovak Hydrometeorological Institute) radar products to identify the DWD equivalent of SHMU's CAPPI 2km product.

**Analysis Date:** September 9, 2025  
**Timestamp Analyzed:** 06:35 UTC (20250909_0635)

## Data Sources

### DWD Products Analyzed
- **dmax**: `composite_dmax_20250909_0635.hd5` (3.6MB)
- **hx**: `composite_hx_20250909_0635.hd5` (2.1MB)

### SHMU Products (Reference)
- **CAPPI 2km**: `cappi2km_20250909063500.hdf` (201KB)

## Technical Specifications Comparison

| Specification | DWD dmax | DWD hx | SHMU CAPPI 2km |
|---------------|----------|--------|----------------|
| **File Size** | 3,678.4 KB | 2,165.3 KB | 201.0 KB |
| **Grid Size** | 4800 × 4400 | 4800 × 4400 | 1560 × 2270 |
| **Data Type** | uint16 | uint16 | uint8 |
| **Coverage** | Germany + surrounding | Germany + surrounding | Slovakia + surrounding |
| **Resolution** | 250m × 250m | 250m × 250m | 331.6m × 482.6m |
| **Projection** | Stereographic | Stereographic | Mercator |
| **ODIM Version** | H5rad 2.3 | H5rad 2.3 | H5rad 2.1 |

## Radar Product Metadata Analysis

### DWD dmax Product
- **Product Name**: `DMax_top_view`
- **Product Type**: `MAX`
- **Quantity**: `DBZH` (Reflectivity)
- **Method**: `MAXIMUM` (camethod)
- **Software**: `POLARA_volans_1.4.008`
- **Pattern**: `DMax`
- **Radar Network**: 17 radars ('deasb','deboo','dedrs','deeis','deess','defbg','defld','dehnr','deisn','demem','deneu','denhb','deoft','depro','deros','detur','deumd')

### DWD hx Product  
- **Product Name**: `HX_top_view`
- **Product Type**: `MAX`
- **Quantity**: `DBZH` (Reflectivity)
- **Method**: `MAXIMUM` (camethod)
- **Software**: `POLARA_volans_1.4.008`
- **Pattern**: `HX`
- **Radar Network**: Same 17 radars as dmax

### SHMU CAPPI 2km Product
- **Product Name**: Not explicitly named
- **Product Type**: `CAPPI`
- **Quantity**: `DBZH` (Reflectivity)
- **Software**: System `SKCOMP` v5.43.14
- **Radar Network**: 4 radars ('CZSKA','SKJAV','SKKOJ','SKLAZ')

## Key Findings

### 1. Product Type Analysis
- **DWD dmax & hx**: Both are `MAX` products, indicating maximum value composites
- **SHMU CAPPI 2km**: Explicitly labeled as `CAPPI` product

### 2. Altitude Information
- **DWD Products**: No explicit altitude information found in metadata
  - Both products use `MAXIMUM` method (camethod)
  - Product names suggest "top_view" perspective
  - Likely represent column maximum reflectivity (similar to SHMU's ZMAX)

- **SHMU CAPPI 2km**: 
  - Explicitly identified as CAPPI product
  - Assumed to represent reflectivity at 2km altitude
  - Uses Mercator projection optimized for Slovakia region

### 3. Technical Differences
- **Data Scaling**: 
  - DWD: gain=0.0029, offset=-64.0, nodata=65535
  - SHMU: gain=0.502, offset=-32.5, nodata=-1.0

- **Geographic Coverage**:
  - DWD: Covers all of Germany (45.7°N-55.9°N, 1.5°E-18.7°E)
  - SHMU: Focuses on Slovakia region (46.0°N-50.7°N, 13.6°E-23.8°E)

## Conclusion: DWD CAPPI 2km Equivalent

### ❌ **Neither DWD dmax nor hx is a CAPPI 2km equivalent**

**Reasoning:**

1. **Product Type Mismatch**: Both DWD products are `MAX` (maximum) products, not CAPPI products
2. **Processing Method**: Both use `MAXIMUM` method indicating column maximum processing
3. **No Altitude Specification**: Neither product specifies a constant altitude level
4. **Product Names**: "DMax_top_view" and "HX_top_view" suggest maximum/top-view composites

### 🔍 **What DWD products likely represent:**
- **dmax**: Likely equivalent to SHMU's **ZMAX** (Column Maximum Reflectivity)
- **hx**: Possibly another form of maximum composite or hybrid product

### 📋 **Recommendations for finding DWD CAPPI 2km equivalent:**

1. **Check other DWD products**: Look for products with names like "PZ" (mentioned in documentation as 3km CAPPI)
2. **Search for CAPPI-specific products**: Products that explicitly mention altitude levels
3. **Look for volumetric data**: Products that might contain multiple altitude levels
4. **Check DWD's local radar products**: Individual radar sites might have CAPPI products

### 🎯 **Key Insight**
DWD appears to use different naming conventions and processing methods compared to SHMU. The searched products (dmax, hx) are maximum composite products rather than constant-altitude products, making them more similar to SHMU's ZMAX than CAPPI 2km.

## Next Steps

To find the true DWD equivalent of SHMU CAPPI 2km:

1. Investigate DWD's "PZ" product (3km CAPPI mentioned in documentation)
2. Check if DWD provides 2km CAPPI products under different naming
3. Examine DWD's volumetric radar data for altitude-specific products
4. Contact DWD directly for product documentation clarification

## Data Processing Notes

- Both DWD products use sophisticated POLARA software framework
- Stereographic projection provides better accuracy for Germany's latitude
- Higher spatial resolution (250m) compared to SHMU (330-480m)
- Larger file sizes indicate more detailed/comprehensive data coverage