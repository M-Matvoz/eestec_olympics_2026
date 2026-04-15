import numpy as np

# Definirajte svoj bounding box (primer za Slovenijo)
lat_min, lat_max = 45.4215, 46.8767
lon_min, lon_max = 13.3755, 16.5967

res_km = 5.0
d_lat = res_km / 111.0  # 1 stopinja lat je cca 111km
# Pri 46°N je 1 stopinja lon cca 77km
d_lon = res_km / (111.0 * np.cos(np.radians(46)))

# Število točk po oseh
lats = np.arange(lat_min, lat_max, d_lat)
lons = np.arange(lon_min, lon_max, d_lon)

n_lon = len(lons)
max_points = 50

# Izračun, koliko vrstic (latitud) gre v en pravokotnik
rows_per_chunk = max_points // n_lon

if rows_per_chunk == 0:
    print(f"Opozorilo: Ena vrstica ima že več kot {max_points} točk! Treba bo deliti po dolžini.")
    rows_per_chunk = 1

chunks = []
for i in range(0, len(lats), rows_per_chunk):
    current_lats = lats[i : i + rows_per_chunk]
    if len(current_lats) == 1:
        print(f"Opozorilo: Zadnji chunk ima samo eno latitudno vrstico. Povečujem max lat za polovico resolucije.")
        current_lats = np.append(current_lats, current_lats[0] + d_lat / 2)
    num_points = len(current_lats) * len(lons)
    chunks.append(
        {
            "min_lat": current_lats[0],
            "max_lat": current_lats[1],
            "min_lon": lons[0],
            "max_lon": lons[-1],
            "num_points": num_points,
        }
    )

# Izpis rezultatov
rectangles = "["
for idx, c in enumerate(chunks):
    rectangles += f'{{"min_lat": {c["min_lat"]}, "max_lat": {c["max_lat"]}, "min_lon": {c["min_lon"]}, "max_lon": {c["max_lon"]}, "num_points": {c["num_points"]}}}'
    if idx < len(chunks) - 1:
        rectangles += ",\n"
rectangles += "]"
print(rectangles)
