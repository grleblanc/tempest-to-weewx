"""t2wee.py - A utility to import your WeatherFlow tempest data into weewx."""

import argparse
import csv
import datetime
import logging
import time
import os
import requests

# --- Configuration ---
# Default values - can be overridden by command-line arguments or environment variables
DEFAULT_API_TOKEN = "YOUR_API_TOKEN"  # Replace with a placeholder
DEFAULT_STATION_ID = "YOUR_STATION_ID"  # Replace with a placeholder
DEFAULT_START_DATE = "2020-01-01"  # YYYY-MM-DD
DEFAULT_OUTPUT_FILE = "wx.csv"
# --- End Configuration ---

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_tempest_data(start_ts, end_ts, api_token, station_id):
    """Fetches weather data from the Tempest API."""

    url = f"https://swd.weatherflow.com/swd/rest/observations/station/{station_id}"

    headers = {
        "Content-Type": "application/json",
    }

    params = {
        "time_start": start_ts,
        "time_end": end_ts,
        "token": api_token,
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("obs", [])

    except requests.exceptions.RequestException as e:
        logging.error("Error fetching Tempest data: %s", e)  # Use logging
        return []
    except ValueError as e:
        logging.error("Error parsing JSON: %s", e)
        return []


def insert_into_csv(data, output_file):
    """Inserts Tempest data into a CSV file, mapping to a Weewx-like schema."""
    try:
        # Check if the file exists to determine whether to write headers
        file_exists = os.path.exists(output_file)

        with open(output_file, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            # Write headers only if the file is newly created
            if not file_exists:
                writer.writerow(
                    [
                        "dateTime",
                        "outTemp",
                        "windSpeed",
                        "windGust",
                        "windDir",
                        "barometer",
                        "outHumidity",
                        "rain",
                        "UV",
                        "radiation",
                        "lightning_distance",
                    ]
                )

            for obs in data:
                # Map Tempest data to Weewx-like schema (with conversions)
                weewx_data = {
                    "dateTime": obs[0],  # Timestamp
                    "outTemp": (
                        (obs[8] * 9 / 5) + 32 if obs[8] is not None else None
                    ),  # C to F
                    "windSpeed": (
                        obs[3] * 2.23694 if obs[3] is not None else None
                    ),  # m/s to mph
                    "windGust": (
                        obs[4] * 2.23694 if obs[4] is not None else None
                    ),  # m/s to mph
                    "windDir": obs[5],
                    "barometer": (
                        obs[6] * 0.02953 if obs[6] is not None else None
                    ),  # hPa to inHg
                    "outHumidity": obs[9],
                    "rain": (
                        obs[13] / 25.4 if obs[13] is not None else None
                    ),  # mm to inches
                    "UV": obs[11],
                    "radiation": obs[12],
                    "lightning_distance": (
                        obs[17] * 0.621371 if obs[17] is not None else None
                    ),  # km to miles,
                }
                writer.writerow(weewx_data.values())  # Write data (values)

    except Exception as e:
        logging.exception("Error writing to CSV: %s", e)  # Use logging


def main(api_token, station_id, start_date_str, output_file):
    """Main function to orchestrate data retrieval and insertion."""

    try:
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    except ValueError:
        logging.error("Invalid start date format.  Use YYYY-MM-DD.")
        return

    end_date = datetime.datetime.now()
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())

    current_ts = start_ts
    interval = 86400  # 1 day

    while current_ts < end_ts:
        try:
            next_ts = min(current_ts + interval, end_ts)
            tempest_data = get_tempest_data(current_ts, next_ts, api_token, station_id)
            if tempest_data:
                insert_into_csv(tempest_data, output_file)
                logging.info(
                    "Finished writing data for %s", datetime.datetime.fromtimestamp(current_ts)
                )
            else:
                logging.warning(
                    "No data retrieved for timestamp range: %s", {current_ts} - {next_ts}
                )
            current_ts = next_ts
            time.sleep(1)  # Be nice to the API

        except Exception as e:
            logging.exception("Error during processing: %s", e)
            logging.error(
                "Error retrieving/writing results for %s", datetime.datetime.fromtimestamp(current_ts)"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill Weewx database with Tempest data."
    )
    parser.add_argument(
        "--api_token",
        default=os.environ.get("TEMPEST_API_TOKEN", DEFAULT_API_TOKEN),
        help="Your Tempest API token (defaults to env var TEMPEST_API_TOKEN or placeholder)",
    )
    parser.add_argument(
        "--station_id",
        default=os.environ.get("TEMPEST_STATION_ID", DEFAULT_STATION_ID),
        help="Your Tempest station ID (defaults to env var TEMPEST_STATION_ID or placeholder)",
    )
    parser.add_argument(
        "--start_date",
        default=DEFAULT_START_DATE,
        help="Start date for backfill (YYYY-MM-DD, defaults to 2020-08-07)",
    )
    parser.add_argument(
        "--output_file",
        default=DEFAULT_OUTPUT_FILE,
        help="Output CSV file (defaults to wx.csv)",
    )

    args = parser.parse_args()

    # Check for required arguments being placeholders
    if args.api_token == "YOUR_API_TOKEN" or args.station_id == "YOUR_STATION_ID":
        logging.error(
            "API token and Station ID must be set via command line arguments or environment variables."
        )
        exit(1)  # Exit with an error code

    main(args.api_token, args.station_id, args.start_date, args.output_file)
