import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from scipy.spatial import cKDTree
from geopy.distance import geodesic
from helper_utils import load_raw_data, load_port_info, load_port_polygons
from data_cleaning import remove_NA, remove_invalid_lat_and_lon, remove_invalid_speed


def load_and_clean_data(parquet_file_path):
    """Load and clean AIS raw data."""
    ais_raw_data = load_raw_data(parquet_file_path)

    # Remove invalid data and duplicates
    ais_raw_data = remove_NA(ais_raw_data)
    ais_raw_data = remove_invalid_lat_and_lon(ais_raw_data)
    ais_raw_data = remove_invalid_speed(ais_raw_data)
    ais_raw_data = ais_raw_data.drop_duplicates()
    return ais_raw_data


def investigate_data(data, name="Data"):
    """Print unique values and null counts for a dataset."""
    print(f"Investigating {name}:")
    for column in data:
        unique_vals = np.unique(data[column])
        nr_values = len(unique_vals)
        if nr_values < 12:
            print(f'The number of values for feature {column}: {nr_values} -- {unique_vals}')
        else:
            print(f'The number of values for feature {column}: {nr_values}')
    print("\nNull value counts:")
    print(data.isnull().sum())


def create_proximity_columns(ais_raw_data, port_info):
    """Create proximity columns and assign nearest port information."""
    # Prepare data
    vessel_coords = ais_raw_data[['latitude', 'longitude']].to_numpy()
    port_coords = port_info[['Latitude', 'Longitude']].to_numpy()
    port_names = port_info['Main Port Name'].to_numpy()

    # Build KD-Tree for efficient nearest neighbor search
    port_tree = cKDTree(port_coords)
    distances, indices = port_tree.query(vessel_coords, k=1)

    # Add proximity information
    ais_raw_data['Distance to Nearest Port'] = distances
    ais_raw_data['Proximity Port Name'] = [port_names[i] for i in indices]
    ais_raw_data['Port Latitude'] = [port_coords[i][0] for i in indices]
    ais_raw_data['Port Longitude'] = [port_coords[i][1] for i in indices]

    # Add proximity columns with thresholds
    for dist, label in zip([1, 3, 5, 10], ['1 km', '3 km', '5 km', '10 km']):
        ais_raw_data[f'Less than {label} from port'] = np.where(ais_raw_data['Distance to Nearest Port'] <= dist, "Yes", "No")

    # Remove port data for vessels not near any port
    ais_raw_data.loc[
        (ais_raw_data['Less than 1 km from port'] == "No") &
        (ais_raw_data['Less than 3 km from port'] == "No") &
        (ais_raw_data['Less than 5 km from port'] == "No") &
        (ais_raw_data['Less than 10 km from port'] == "No"),
        ['Proximity Port Name', 'Port Latitude', 'Port Longitude']
    ] = ["No", np.nan, np.nan]

    return ais_raw_data


def process_with_polygons(ais_raw_data, polygons, chunk_size=10000):
    """Check vessel locations against port polygons using spatial joins."""
    # Ensure correct CRS
    polygons = polygons.set_geometry("polygon").to_crs("EPSG:4326")

    def process_chunk(chunk):
        chunk['geometry'] = gpd.points_from_xy(chunk['longitude'], chunk['latitude'])
        vessels_gdf = gpd.GeoDataFrame(chunk, geometry='geometry', crs="EPSG:4326")

        # Perform spatial join
        joined = gpd.sjoin(vessels_gdf, polygons, how='left', predicate='within')
        joined['Parked in Port'] = joined['index_right'].notnull()
        joined['Port Name'] = joined['port_name'].where(joined['Parked in Port'], "No")
        return joined.drop(columns=['geometry', 'index_right'])

    # Process in chunks
    results = []
    for i in range(0, len(ais_raw_data), chunk_size):
        chunk = ais_raw_data.iloc[i:i + chunk_size]
        results.append(process_chunk(chunk))

    # Combine results
    return pd.concat(results, ignore_index=True)


def main():
    # File paths
    parquet_file_path = "data/raw_data/raw_data.parquet"

    # Load and clean AIS data
    ais_raw_data = load_and_clean_data(parquet_file_path)

    # Load port data and polygons
    port_info = pd.DataFrame(load_port_info()).drop_duplicates()
    polygons = load_port_polygons().drop_duplicates()

    # Investigate data
    investigate_data(ais_raw_data, "AIS Raw Data")
    investigate_data(port_info, "Port Information")

    # Create proximity columns
    ais_raw_data = create_proximity_columns(ais_raw_data, port_info)

    # Process spatial join with polygons
    final_results = process_with_polygons(ais_raw_data, polygons)

    # Save final results
    final_results.to_csv('vessels_with_parked_status.csv', index=False)
    print("Processing completed successfully!")


if __name__ == "__main__":
    main()
