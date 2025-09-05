#!/usr/bin/env python3
"""
MVP SHMU Radar Data Processor for JavaScript Frontend
Processes SHMU HDF5 radar data using PyArt and prepares it for web consumption
"""
import h5py
import numpy as np
import json
import requests
from datetime import datetime
import os
import warnings

warnings.filterwarnings("ignore")


class SHMURadarProcessor:
    """
    MVP processor for SHMU radar data with frontend preparation
    """

    def __init__(self):
        self.type_mapping = {
            "PABV": "zmax",  # Maximum Reflectivity
            "PANV": "cappi2km",  # CAPPI 2km
            "PADV": "etop",  # Echo Top Height
            "PASV": "pac01",  # 1h Accumulated Precipitation
        }

        self.product_info = {
            "PABV": {"name": "ZMAX", "description": "Column Maximum Reflectivity"},
            "PANV": {
                "name": "CAPPI 2km",
                "description": "Reflectivity at 2km altitude",
            },
            "PADV": {"name": "Echo Top", "description": "Echo Top Height"},
            "PASV": {
                "name": "Precipitation",
                "description": "1-hour Accumulated Precipitation",
            },
        }

    def download_shmu_data(self, product_type, timestamp, output_dir="data"):
        """
        Download SHMU radar data for specified product and timestamp

        Args:
            product_type (str): PABV, PANV, PADV, or PASV
            timestamp (str): Format YYYYMMDDHHMMSS
            output_dir (str): Directory to save downloaded files

        Returns:
            str: Path to downloaded file
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        base_url = "https://opendata.shmu.sk/meteorology/weather/radar/composite/skcomp"
        date_str = timestamp[:8]  # YYYYMMDD

        url = f"{base_url}/{self.type_mapping[product_type]}/{date_str}/T_{product_type}22_C_LZIB_{timestamp}.hdf"
        filename = os.path.join(
            output_dir, f"T_{product_type}22_C_LZIB_{timestamp}.hdf"
        )

        print(f"Downloading {product_type} data from: {url}")

        try:
            # Disable SSL verification for SHMU API
            response = requests.get(url, verify=False, timeout=30)
            response.raise_for_status()

            with open(filename, "wb") as f:
                f.write(response.content)

            print(f"Successfully downloaded: {filename}")
            return filename

        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")
            return None

    def estimate_precipitation_rate(self, dbz_data):
        """
        Convert reflectivity (dBZ) to precipitation rate (mm/h) using Marshall-Palmer Z-R relationship
        Z = 200 * R^1.6, therefore R = (Z/200)^(1/1.6)
        """
        # Convert dBZ to linear reflectivity factor Z
        z_linear = 10.0 ** (dbz_data / 10.0)

        # Apply Z-R relationship
        precip_rate = np.power(z_linear / 200.0, 1.0 / 1.6)

        # Handle invalid values
        precip_rate[np.isnan(dbz_data) | (dbz_data < -10)] = 0

        # Cap extremely high values (likely artifacts)
        precip_rate[precip_rate > 200] = 200

        return precip_rate

    def process_for_frontend(self, hdf_filepath):
        """
        Process HDF5 radar data for JavaScript frontend consumption

        Args:
            hdf_filepath (str): Path to HDF5 file

        Returns:
            dict: Frontend-ready data structure
        """
        print(f"Processing {os.path.basename(hdf_filepath)} for frontend...")

        with h5py.File(hdf_filepath, "r") as f:
            # Read raw data
            data = f["dataset1/data1/data"][:]
            what_attrs = dict(f["dataset1/what"].attrs)
            where_attrs = dict(f["where"].attrs)

            # Apply scaling
            gain = what_attrs.get("gain", 1.0)
            offset = what_attrs.get("offset", 0.0)
            nodata = what_attrs.get("nodata", -32768)

            scaled_data = data.astype(np.float32) * gain + offset

            # Handle nodata values - convert to NaN for JSON serialization
            scaled_data_masked = np.where(data == 0, np.nan, scaled_data)

            # Create coordinate arrays (flip lats for correct orientation)
            ll_lon, ll_lat = where_attrs["LL_lon"], where_attrs["LL_lat"]
            ur_lon, ur_lat = where_attrs["UR_lon"], where_attrs["UR_lat"]

            lons = np.linspace(ll_lon, ur_lon, data.shape[1])
            lats = np.linspace(ur_lat, ll_lat, data.shape[0])  # Flipped

            # Get product information
            product = what_attrs.get("product", b"").decode()
            quantity = what_attrs.get("quantity", b"").decode()

            # Determine units and data type
            if quantity == "DBZH":
                units = "dBZ"
                data_type = "reflectivity"
            elif quantity == "ACRR":
                units = "mm"
                data_type = "precipitation"
            elif quantity == "HGHT":
                units = "km"
                data_type = "height"
            else:
                units = "unknown"
                data_type = quantity.lower()

            # Prepare frontend-ready data structure
            result = {
                "product": product,
                "product_name": self.product_info.get(
                    os.path.basename(hdf_filepath).split("_")[1][:4],
                    {"name": product, "description": ""},
                ),
                "quantity": quantity,
                "data_type": data_type,
                "timestamp": what_attrs.get("startdate", b"").decode()
                + what_attrs.get("starttime", b"").decode(),
                "dimensions": list(data.shape),
                "projection": where_attrs.get("projdef", b"").decode(),
                "extent": [
                    ll_lon,
                    ur_lon,
                    ll_lat,
                    ur_lat,
                ],  # [lon_min, lon_max, lat_min, lat_max]
                "data": scaled_data_masked.tolist(),  # 2D array for JSON
                "coordinates": {"lons": lons.tolist(), "lats": lats.tolist()},
                "data_range": [
                    float(np.nanmin(scaled_data_masked)),
                    float(np.nanmax(scaled_data_masked)),
                ],
                "units": units,
                "nodata_value": float(nodata),
                "metadata": {
                    "source": "SHMU Slovakia",
                    "grid_size": f"{data.shape[1]}x{data.shape[0]}",
                    "resolution_m": [
                        where_attrs.get("xscale", 0),
                        where_attrs.get("yscale", 0),
                    ],
                    "radar_nodes": (
                        f["how"].attrs.get("nodes", b"").decode() if "how" in f else ""
                    ),
                    "processing_time": datetime.now().isoformat(),
                },
            }

            # Add precipitation rate estimation for reflectivity products
            if quantity == "DBZH":
                precip_rate = self.estimate_precipitation_rate(scaled_data_masked)
                result["precipitation_rate"] = {
                    "data": precip_rate.tolist(),
                    "units": "mm/h",
                    "data_range": [
                        float(np.nanmin(precip_rate)),
                        float(np.nanmax(precip_rate)),
                    ],
                    "method": "Marshall-Palmer Z-R relationship",
                }

            print(f"✓ Processed {product} ({quantity})")
            print(f"  Grid size: {data.shape}")
            print(
                f"  Data range: {result['data_range'][0]:.2f} to {result['data_range'][1]:.2f} {units}"
            )

            return result

    def process_multiple_products(
        self, timestamp, products=None, download=True, output_dir="processed"
    ):
        """
        Process multiple radar products for a given timestamp

        Args:
            timestamp (str): Format YYYYMMDDHHMMSS
            products (list): List of product types to process (default: all)
            download (bool): Whether to download data if not present
            output_dir (str): Directory for processed JSON files

        Returns:
            dict: Dictionary of processed products
        """
        if products is None:
            products = ["PABV", "PANV", "PASV"]  # Skip PADV (echo top) for MVP

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        results = {}

        for product_type in products:
            filename = f"T_{product_type}22_C_LZIB_{timestamp}.hdf"
            filepath = os.path.join("data", filename)

            # Download if needed
            if download and not os.path.exists(filepath):
                filepath = self.download_shmu_data(product_type, timestamp)
                if not filepath:
                    continue

            # Process the data
            try:
                radar_data = self.process_for_frontend(filepath)
                results[product_type] = radar_data

                # Save individual JSON file
                output_file = os.path.join(
                    output_dir, f"radar_{product_type.lower()}_{timestamp}.json"
                )
                with open(output_file, "w") as f:
                    json.dump(radar_data, f, indent=2)

                print(f"✓ Saved: {output_file}")

            except Exception as e:
                print(f"✗ Error processing {product_type}: {e}")
                continue

        # Save combined results
        if results:
            combined_file = os.path.join(output_dir, f"radar_all_{timestamp}.json")
            with open(combined_file, "w") as f:
                json.dump(results, f, indent=2)
            print(f"✓ Saved combined: {combined_file}")

        return results

    def create_sample_data(self, timestamp="20250904014500"):
        """
        Create sample processed data for frontend development
        """
        print("Creating sample data for frontend development...")

        # Use existing files if available, otherwise try to download
        results = self.process_multiple_products(timestamp, download=False)

        if not results:
            print("No existing data found. Trying to download sample data...")
            results = self.process_multiple_products(timestamp, download=True)

        if results:
            print(f"\n✓ Created sample data for {len(results)} products")
            print("Available products:")
            for product_type, data in results.items():
                product_info = data["product_name"]
                print(
                    f"  - {product_type}: {product_info['name']} ({product_info['description']})"
                )
        else:
            print("✗ No sample data could be created")

        return results


def main():
    """
    Main function demonstrating the MVP processor
    """
    print("SHMU Radar Data Processor MVP")
    print("=" * 40)

    processor = SHMURadarProcessor()

    # Create sample data using example timestamp
    timestamp = "20250904141500"

    # Process radar data for frontend
    results = processor.create_sample_data(timestamp)

    if results:
        print(f"\n🎉 Successfully processed {len(results)} radar products!")
        print("\nReady for frontend integration:")
        print("- Check the 'processed/' directory for JSON files")
        print("- Use the data structure as documented in DOCUMENTATION.md")
        print("- Integrate with your JavaScript mapping library")
    else:
        print(
            "\n⚠️  No data could be processed. Check network connection and try again."
        )


if __name__ == "__main__":
    main()
