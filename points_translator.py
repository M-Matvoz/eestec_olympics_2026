import pandas as pd


class point:
    def __init__(self, latitude: float, longitude: float, name: str, location: str, status: str, power: str):
        self.latitude = latitude
        self.longitude = longitude
        self.hourly_dataframe = None
        self.name = name
        self.location = location
        self.status = status
        self.power = power

# First, read the CSV file as a pandas dataframe
points_df = pd.read_csv("points.csv")
# Then, create a list of point objects from the dataframe
POINTS = []
for index, row in points_df.iterrows():
    POINTS.append(
        point(
            latitude=row["Latitude"],
            longitude=row["Longitude"],
            name=row["RTP"],
            location=row["Lokacija"],
            status=row["Status"],
            power=row["Moč [MW]"],
        )
    )

# Print the list of points to directly paste in to other files
print("[")
for idx, point in enumerate(POINTS):
    print(
        f'{{"latitude": {point.latitude}, "longitude": {point.longitude}, "name": "{point.name}", "location": "{point.location}", "status": "{point.status}", "power": "{point.power}",}}', end=""
    )
    if idx < len(POINTS) - 1:
        print(",")
print("]")