import time

import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


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

START_DATE = "2006-01-01"
END_DATE = "2016-12-31"
STEP_MONTHS = 6
OUTPUT_DIR = "D:\\FE\\eestec_2026\\weather_data"


def get_month_start_end_dates(index: int) -> tuple[str, str]:
    current_start_date = pd.to_datetime(START_DATE) + pd.DateOffset(months=index * STEP_MONTHS)
    current_end_date = current_start_date + pd.DateOffset(months=STEP_MONTHS) - pd.DateOffset(days=1)

    return current_start_date.strftime("%Y-%m-%d"), current_end_date.strftime("%Y-%m-%d")


class WeatherPointWithData:
    def __init__(self, latitude, longitude, elevation, utc_offset_seconds):
        self.latitude = latitude
        self.longitude = longitude
        self.elevation = elevation
        self.utc_offset_seconds = utc_offset_seconds
        self.hourly_dataframe = None

    def add_hourly_data(self, hourly_dataframe):
        # First, check if the column names are the same as in the existing dataframe, if not, raise an error
        if self.hourly_dataframe is not None:
            if set(hourly_dataframe.columns) != set(self.hourly_dataframe.columns):
                raise ValueError("Column names in the new hourly data do not match the existing dataframe.")
            # If the column names are the same, we can concatenate the dataframes, extending the index to avoid duplicates
            self.hourly_dataframe = pd.concat([self.hourly_dataframe, hourly_dataframe], ignore_index=True)
        else:
            self.hourly_dataframe = hourly_dataframe


lookup_table_points: dict[tuple[float, float], WeatherPointWithData] = {}

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
URL = "https://archive-api.open-meteo.com/v1/archive"


def build_points_lat_lon_lists():
    lats = []
    lons = []
    for point in POINTS:
        lats.append(point["latitude"])
        lons.append(point["longitude"])
    return lats, lons

def __execute_request_for_point(point_index: int, start_end_dates: tuple[str, str]):
    try:
        lats, lons = build_points_lat_lon_lists()
        params = {
            "start_date": start_end_dates[0],
            "end_date": start_end_dates[1],
            "hourly": [
                "shortwave_radiation",
                "is_day",
                "wind_speed_10m",
                "wind_speed_100m",
                "wind_gusts_10m",
                "wind_direction_100m",
                "wind_direction_10m",
            ],
            "models": "cerra",
            "wind_speed_unit": "ms",
            "timeformat": "unixtime",
            "latitude": lats,
            "longitude": lons,
        }
        resp = openmeteo.weather_api(URL, params=params)
        return resp
    except openmeteo_requests.OpenMeteoRequestsError as e:
        reason = str(e).split("{")[1].split("}")[0]
        found = reason.find("limit")
        print(found)
        if found != -1:
            print("Rate exceeded. Resetting cache and retrying")
            cache_session.cache.clear()
            time.sleep(1)
            return __execute_request_for_point(point_index, start_end_dates)
        else:
            print(f"Error for point index {point_index} and dates {start_end_dates}: {e}")
        return None


def get_weather_data_for_point(point_index: int, start_end_dates: tuple[str, str]):
    responses = __execute_request_for_point(point_index, start_end_dates)

    if responses is None:
        print(f"Error: No response for point index {point_index} and dates {start_end_dates}")
        return None

    # Process bounding box locations
    for response in responses:
        point = WeatherPointWithData(
            latitude=response.Latitude(),
            longitude=response.Longitude(),
            elevation=response.Elevation(),
            utc_offset_seconds=response.UtcOffsetSeconds(),
        )

        # add the point to the list of weather data points if one with the same coordinates does not already exist
        if (point.latitude, point.longitude) not in lookup_table_points:
            lookup_table_points[(point.latitude, point.longitude)] = point

        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()
        hourly_shortwave_radiation = hourly.Variables(0).ValuesAsNumpy()
        hourly_is_day = hourly.Variables(1).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(2).ValuesAsNumpy()
        hourly_wind_speed_100m = hourly.Variables(3).ValuesAsNumpy()
        hourly_wind_gusts_10m = hourly.Variables(4).ValuesAsNumpy()
        hourly_wind_direction_100m = hourly.Variables(5).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(6).ValuesAsNumpy()

        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
        }

        hourly_data["shortwave_radiation"] = hourly_shortwave_radiation
        hourly_data["is_day"] = hourly_is_day
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
        hourly_data["wind_speed_100m"] = hourly_wind_speed_100m
        hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
        hourly_data["wind_direction_100m"] = hourly_wind_direction_100m
        hourly_data["wind_direction_10m"] = hourly_wind_direction_10m

        hourly_dataframe = pd.DataFrame(data=hourly_data)

        lookup_table_points[(point.latitude, point.longitude)].add_hourly_data(hourly_dataframe)
    return True


def save_data_to_csv(current_start_end_dates: tuple[str, str]):
    for (latitude, longitude), point in lookup_table_points.items():
        filename = f"{OUTPUT_DIR}\\weather_data_{latitude}_{longitude}_{current_start_end_dates[0]}_{current_start_end_dates[1]}.csv"
        print(f"Saving data for point ({latitude}, {longitude}) to {filename}...")
        point.hourly_dataframe.to_csv(filename, index=False)
        print(f"Saved data for point ({latitude}, {longitude}) to {filename}")


def main():
    current_start_end_dates = get_month_start_end_dates(0)

    while pd.to_datetime(current_start_end_dates[0]) < pd.to_datetime(END_DATE):
        print(f"Processing timeframe of {current_start_end_dates[0]} to {current_start_end_dates[1]}")
        for point_index in range(len(POINTS)):
            print("Processing point index", point_index)
            data = get_weather_data_for_point(point_index, current_start_end_dates)
            if data is None:
                print(f"Error: No data for point index {point_index} and dates {current_start_end_dates}")
                return
        # Increase the start and end dates for the next iteration
        print(f"Finished processing timeframe of {current_start_end_dates[0]} to {current_start_end_dates[1]}")
        current_start_end_dates = get_month_start_end_dates((pd.to_datetime(current_start_end_dates[0]) - pd.to_datetime(START_DATE)).days // (STEP_MONTHS * 30) + 1)
        save_data_to_csv(current_start_end_dates)
        # Clear the lookup table for the next iteration to save memory
        lookup_table_points.clear()


if __name__ == "__main__":
    main()
