import pandas as pd

POINTS = [
    {"latitude": 46.1, "longitude": 14.588, "name": "Beričevo", "location": "Ljubljana", "status": "Obstoječe", "power": "150",},
    {"latitude": 45.684, "longitude": 13.969, "name": "Divača", "location": "Primorska", "status": "Obstoječe", "power": "150",},
    {"latitude": 45.958, "longitude": 15.491, "name": "Krško", "location": "Posavje", "status": "Obstoječe", "power": "150",},
    {"latitude": 46.395, "longitude": 15.794, "name": "Cirkovce", "location": "Ptuj", "status": "Obstoječe", "power": "150",},
    {"latitude": 46.554, "longitude": 15.646, "name": "Maribor", "location": "Maribor", "status": "Obstoječe", "power": "100",},
    {"latitude": 46.25, "longitude": 14.35, "name": "Okroglo", "location": "Gorenjska", "status": "Obstoječe", "power": "100",},
    {"latitude": 46.083, "longitude": 14.475, "name": "Kleče", "location": "Ljubljana", "status": "Obstoječe", "power": "100",},
    {"latitude": 46.395, "longitude": 15.794, "name": "Kidričevo", "location": "Ptuj", "status": "Obstoječe", "power": "100",},
    {"latitude": 46.391, "longitude": 15.573, "name": "Slovenska Bistrica", "location": "Štajerska", "status": "Obstoječe", "power": "100",},
    {"latitude": 45.887, "longitude": 13.909, "name": "Ajdovščina", "location": "Primorska", "status": "Obstoječe", "power": "100",},
    {"latitude": 45.912, "longitude": 13.64, "name": "Vrtojba", "location": "Nova Gorica", "status": "Obstoječe", "power": "100",},
    {"latitude": 46.183, "longitude": 13.733, "name": "Tolmin", "location": "Posočje", "status": "Obstoječe", "power": "100",},
    {"latitude": 45.681, "longitude": 14.197, "name": "Pivka", "location": "Primorska", "status": "Obstoječe", "power": "75",},
    {"latitude": 46.035, "longitude": 13.585, "name": "Plave", "location": "Nova Gorica", "status": "Obstoječe", "power": "75",},
    {"latitude": 46.056, "longitude": 14.505, "name": "LCL", "location": "Ljubljana", "status": "Novo", "power": "80",},
    {"latitude": 46.068, "longitude": 14.54, "name": "Vodenska", "location": "Ljubljana", "status": "Novo", "power": "60",},
    {"latitude": 46.072, "longitude": 14.471, "name": "Brdo", "location": "Ljubljana (Brdo)", "status": "Novo", "power": "80",},
    {"latitude": 46.034, "longitude": 14.541, "name": "Rudnik", "location": "Ljubljana (Rudnik)", "status": "Novo", "power": "80",},
    {"latitude": 46.06, "longitude": 14.566, "name": "Vevče", "location": "Ljubljana (Vevče)", "status": "Novo", "power": "80",},
    {"latitude": 46.44, "longitude": 14.21, "name": "Vrtača", "location": "Gorenjska", "status": "Novo", "power": "60",},
    {"latitude": 46.135, "longitude": 14.745, "name": "Moravče", "location": "Osrednja Slovenija", "status": "Novo", "power": "60",},
    {"latitude": 45.548, "longitude": 13.73, "name": "Luka Koper", "location": "Koper", "status": "Novo", "power": "100",},
    {"latitude": 46.383, "longitude": 15.388, "name": "Zreče", "location": "Štajerska", "status": "Novo", "power": "80",},
    {"latitude": 46.521, "longitude": 14.854, "name": "Mežica", "location": "Koroška", "status": "Novo", "power": "60",},
    {"latitude": 46.224, "longitude": 14.457, "name": "Brnik", "location": "Gorenjska (Letališče)", "status": "Novo", "power": "60",},
    {"latitude": 46.165, "longitude": 14.306, "name": "Trata", "location": "Škofja Loka", "status": "Novo", "power": "60",},
    {"latitude": 46.653, "longitude": 16.35, "name": "Dobrovnik", "location": "Prekmurje", "status": "Novo", "power": "80",},
    {"latitude": 46.56, "longitude": 15.672, "name": "Pobrežje", "location": "Maribor", "status": "Novo", "power": "80",},
    {"latitude": 46.532, "longitude": 15.592, "name": "Pekre", "location": "Maribor", "status": "Novo", "power": "80",},
    {"latitude": 46.395, "longitude": 15.794, "name": "Kidričevo", "location": "Ptuj", "status": "Novo", "power": "80",},
]

INPUT_DIR = "D:\\FE\\eestec_2026\\weather_data"

# Read all CSV files in the input directory and print their names out. Grou them by location and print the number of files for each location.
import os
from collections import defaultdict

location_files = defaultdict(list)

for filename in os.listdir(INPUT_DIR):
    if filename.endswith(".csv"):
        filepath = os.path.join(INPUT_DIR, filename)
        # Extract location from filename (assuming format: weather_data_{latitude}_{longitude}_{start_date}_{end_date}.csv)
        parts = filename.split("_")
        if len(parts) >= 5:
            lat = float(parts[2])  # Assuming latitude is the second part of the filename
            lon = float(parts[3])  # Assuming longitude is the third part of the filename
            # Find the corresponding point in POINTS based on latitude and longitude
            for point in POINTS:
                deviation_lat = abs(point["latitude"] - lat)
                deviation_lon = abs(point["longitude"] - lon)
                if deviation_lat < 0.04 and deviation_lon < 0.04:
                    location_files[point["name"]].append(filepath)
                    break
print("\nFiles grouped by location:")
for location, files in location_files.items():
    if len(files) == 0:
        print(f"{location}: No files found")
        continue
    else:
        print(f"{location}: {len(files)} files")
        print("Files:")
        for filepath in files:
            print(f"  - {os.path.basename(filepath)}")

    reduce = False

    if len(files) == 14:
        print(f"Warning: Duplicate files found for location {location}. There should only be 7 files (one for each timeframe).")
        reduce = True

    # Read all the files as dataframes and concatenate them into one dataframe for the location. Then save the concatenated dataframe to a new CSV file named {location}_combined.csv in the input directory. The values should be multiplied by the factor (1 if there are 7 files, 0.5 if there are 14 files) to account for the duplicate files. Finally, print out the number of rows in the combined dataframe for each location.

    combined_df = pd.DataFrame()
    i = 0
    for filepath in files:
        i+=1
        if reduce and i >= 8:
            continue
        df = pd.read_csv(filepath)
        
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    combined_df.to_csv(os.path.join(INPUT_DIR, f"{location}_combined.csv"), index=False)
    print(f"Combined dataframe for location {location} saved to {location}_combined.csv with {len(combined_df)} rows.")
