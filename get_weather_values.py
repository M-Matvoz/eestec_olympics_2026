import pandas as pd
import os
import math
INPUT_DIR = "D:\\FE\\eestec_2026\\combined_data"
files = os.listdir(INPUT_DIR)
ranking_dict = {}
# for file in files:
#     if file.endswith(".csv"):
#         df = pd.read_csv(os.path.join(INPUT_DIR, file))
#         total_shortwave = df["shortwave_radiation"].sum() * 20/7  # 7 out of 20 intervals for 10 years
#         total_per_year_per_m2 = total_shortwave / 10
#         ranking_dict[file] = total_per_year_per_m2
# # Sort the ranking dictionary by total shortwave radiation in descending order
# sorted_ranking = sorted(ranking_dict.items(), key=lambda x: x[1], reverse=True)
# print("\nRanking of locations based on total shortwave radiation:")
# for rank, (file, total_per_year_per_m2) in enumerate(sorted_ranking, start=1):
#     print(f"{rank}. {file}: {total_per_year_per_m2:.2f} W/m² per year")

formula = ""

def calculate_energy_for_speed(wind_speed):
    # Placeholder for actual energy calculation logic
    if wind_speed < 4 or wind_speed > 25:
        return 0
    if wind_speed > 19:
        wind_speed = 19
    P = 0.9 * 0.593 * 1/2 * 1.275 * 35**2 * math.pi  * (wind_speed ** 3)
    return P

for file in files:
    if file.endswith(".csv"):
        df = pd.read_csv(os.path.join(INPUT_DIR, file))
        wind_speed = df["wind_speed_10m"].fillna(0)  # Replace NaN values with 0
        power = wind_speed.apply(calculate_energy_for_speed)
        total_energy = power.sum() * 20/7  # 7 out of 20 intervals for 10 years
        total_energy_per_year = total_energy / 10
        ranking_dict[file] = total_energy_per_year
# Sort the ranking dictionary by total energy in descending order
sorted_ranking = sorted(ranking_dict.items(), key=lambda x: x[1], reverse=True)
print("\nRanking of locations based on total energy from wind:")
for rank, (file, total_energy_per_year) in enumerate(sorted_ranking, start=1):
    print(f"{rank}. {file}: {total_energy_per_year:.2f} Wh per year")