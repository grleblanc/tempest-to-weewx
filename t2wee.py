"""t2wee.py - A utility to import your WeatherFlow tempest data into weewx."""

import argparse
import csv
import datetime
import logging
import time
import os
import requests
import sqlite3
from typing import List, Dict, Any

# --- Configuration ---
# Default values - can be overridden by command-line arguments or environment variables
DEFAULT_API_TOKEN = "0a36ab0b-baf7-4a0f-a108-e78119ccba96"  # Replace with a placeholder
DEFAULT_DEVICE_ID = "83997"  # Tempest device ID (not station ID!)
DEFAULT_START_DATE = "2020-01-01"  # YYYY-MM-DD
DEFAULT_OUTPUT_FILE = "wx.csv"
DEFAULT_DB_PATH = "/data/archive/weewx.sdb"  # WeeWX database path
DEFAULT_INTERVAL = 5  # 5-minute archive interval (standard for WeeWX)
DEFAULT_US_UNITS = 1  # 1 = US customary units (F, mph, inHg)
# --- End Configuration ---

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_tempest_data(start_ts, end_ts, api_token, device_id):
    """Fetches weather data from the Tempest API using device endpoint.
    
    Note: Use device_id (e.g., 83997) NOT station_id (e.g., 25998)
    The device endpoint supports historical data, station endpoint does not.
    """

    url = f"https://swd.weatherflow.com/swd/rest/observations/device/{device_id}"

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
        
        # Device endpoint returns obs as arrays, not dicts
        obs_arrays = data.get("obs", [])
        
        # Handle None or missing obs data
        if obs_arrays is None:
            return []
        
        # Convert array format to dict format for compatibility
        # Array indices: [0]=timestamp, [1]=lull, [2]=avg, [3]=gust, [4]=dir, [5]=pressure, 
        # [6]=temp, [7]=humidity, [8]=illuminance, [9]=uv, [10]=solar, [11]=rain_prev_min,
        # [12]=precip_type, [13]=lightning_avg_dist, [14]=lightning_count, [15]=battery, [16]=interval
        
        converted_obs = []
        for obs in obs_arrays:
            if len(obs) >= 17:  # Ensure we have all fields
                converted_obs.append({
                    'timestamp': obs[0],
                    'wind_lull': obs[1],
                    'wind_avg': obs[2],
                    'wind_gust': obs[3],
                    'wind_direction': obs[4],
                    'station_pressure': obs[5],
                    'air_temperature': obs[6],
                    'relative_humidity': obs[7],
                    'illuminance': obs[8],
                    'uv': obs[9],
                    'solar_radiation': obs[10],
                    'precip': obs[11],  # rain in previous minute
                    'precip_type': obs[12],
                    'lightning_strike_avg_distance': obs[13],
                    'lightning_strike_count': obs[14],
                    'battery': obs[15],
                    'report_interval': obs[16],
                })
        
        return converted_obs

    except requests.exceptions.RequestException as e:
        logging.error("Error fetching Tempest data: %s", e)
        return []
    except ValueError as e:
        logging.error("Error parsing JSON: %s", e)
        return []
    except IndexError as e:
        logging.error("Error parsing observation array: %s", e)
        return []


def convert_tempest_to_weewx(obs: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a single Tempest observation to WeeWX format.
    
    Tempest API returns observations as dictionaries with named keys.
    """
    return {
        "dateTime": obs.get('timestamp'),  # Unix epoch timestamp
        "usUnits": DEFAULT_US_UNITS,  # US customary units
        "interval": DEFAULT_INTERVAL,  # 5-minute archive interval
        "outTemp": (obs.get('air_temperature') * 9 / 5) + 32 if obs.get('air_temperature') is not None else None,  # C to F
        "windSpeed": obs.get('wind_avg') * 2.23694 if obs.get('wind_avg') is not None else None,  # m/s to mph
        "windGust": obs.get('wind_gust') * 2.23694 if obs.get('wind_gust') is not None else None,  # m/s to mph
        "windDir": obs.get('wind_direction'),  # degrees
        "barometer": obs.get('sea_level_pressure') * 0.02953 if obs.get('sea_level_pressure') is not None else None,  # hPa to inHg
        "outHumidity": obs.get('relative_humidity'),  # percent
        "rain": obs.get('precip') / 25.4 if obs.get('precip') is not None else None,  # mm to inches
        "UV": obs.get('uv'),  # UV index
        "radiation": obs.get('solar_radiation'),  # W/m²
        "lightning_distance": obs.get('lightning_strike_last_distance') * 0.621371 if obs.get('lightning_strike_last_distance') is not None else None,  # km to miles
    }


def insert_into_database(data: List[List[Any]], db_path: str) -> int:
    """Insert Tempest data directly into WeeWX SQLite database."""
    inserted_count = 0
    skipped_count = 0
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        for obs in data:
            weewx_record = convert_tempest_to_weewx(obs)
            date_time = weewx_record['dateTime']
            
            # Check if record already exists
            cursor.execute("SELECT COUNT(*) FROM archive WHERE dateTime = ?", (date_time,))
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                skipped_count += 1
                continue
            
            # Insert the record
            columns = ', '.join(weewx_record.keys())
            placeholders = ', '.join(['?' for _ in weewx_record])
            query = f"INSERT INTO archive ({columns}) VALUES ({placeholders})"
            
            cursor.execute(query, list(weewx_record.values()))
            inserted_count += 1
        
        conn.commit()
        conn.close()
        
        logging.info(f"Database insert: {inserted_count} inserted, {skipped_count} skipped (duplicates)")
        return inserted_count
        
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
        return 0
    except Exception as e:
        logging.exception(f"Error writing to database: {e}")
        return 0


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
                        "usUnits",
                        "interval",
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
                weewx_record = convert_tempest_to_weewx(obs)
                # Write only the columns we defined in the header
                writer.writerow([
                    weewx_record['dateTime'],
                    weewx_record['usUnits'],
                    weewx_record['interval'],
                    weewx_record['outTemp'],
                    weewx_record['windSpeed'],
                    weewx_record['windGust'],
                    weewx_record['windDir'],
                    weewx_record['barometer'],
                    weewx_record['outHumidity'],
                    weewx_record['rain'],
                    weewx_record['UV'],
                    weewx_record['radiation'],
                    weewx_record['lightning_distance'],
                ])

    except Exception as e:
        logging.exception("Error writing to CSV: %s", e)  # Use logging


def main(api_token, device_id, start_date_str, output_file, db_path=None, mode='csv'):
    """Main function to orchestrate data retrieval and insertion.
    
    Args:
        api_token: WeatherFlow API token
        device_id: WeatherFlow device ID (e.g., 83997 for Tempest sensor)
        start_date_str: Start date in YYYY-MM-DD format
        output_file: Output CSV file (for csv mode)
        db_path: Path to WeeWX SQLite database (for db mode)
        mode: 'csv' or 'db' - determines output method
    """

    try:
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    except ValueError:
        logging.error("Invalid start date format.  Use YYYY-MM-DD.")
        return

    end_date = datetime.datetime.now()
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())

    current_ts = start_ts
    interval = 86400  # 1 day chunks
    total_inserted = 0
    total_skipped = 0

    logging.info(f"Starting backfill in '{mode}' mode from {start_date_str} to now")
    if mode == 'db':
        logging.info(f"Target database: {db_path}")
    else:
        logging.info(f"Target CSV file: {output_file}")

    while current_ts < end_ts:
        try:
            next_ts = min(current_ts + interval, end_ts)
            tempest_data = get_tempest_data(current_ts, next_ts, api_token, device_id)
            
            if tempest_data:
                if mode == 'db':
                    inserted = insert_into_database(tempest_data, db_path)
                    total_inserted += inserted
                    total_skipped += (len(tempest_data) - inserted)
                else:
                    insert_into_csv(tempest_data, output_file)
                    total_inserted += len(tempest_data)
                
                logging.info(
                    "Processed %d records for %s", 
                    len(tempest_data),
                    datetime.datetime.fromtimestamp(current_ts)
                )
            else:
                logging.warning(
                    "No data retrieved for timestamp range: %s to %s", 
                    datetime.datetime.fromtimestamp(current_ts),
                    datetime.datetime.fromtimestamp(next_ts)
                )
            current_ts = next_ts
            time.sleep(5)  # Be nice to the API (5s delay to avoid 429 rate limits)

        except Exception as e:
            logging.exception("Error during processing: %s", e)
            logging.error(
                "Error retrieving/writing results for %s", datetime.datetime.fromtimestamp(current_ts)
            )
    
    # Summary
    logging.info("="*60)
    logging.info("BACKFILL COMPLETE")
    logging.info(f"Total records inserted: {total_inserted}")
    if mode == 'db':
        logging.info(f"Total records skipped (duplicates): {total_skipped}")
    logging.info("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill WeeWX database with Tempest data."
    )
    parser.add_argument(
        "--api_token",
        default=os.environ.get("TEMPEST_API_TOKEN", DEFAULT_API_TOKEN),
        help="Your Tempest API token (defaults to env var TEMPEST_API_TOKEN)",
    )
    parser.add_argument(
        "--device_id",
        default=os.environ.get("TEMPEST_DEVICE_ID", DEFAULT_DEVICE_ID),
        help="Your Tempest device ID (NOT station ID - e.g., 83997 for Tempest sensor)",
    )
    parser.add_argument(
        "--start_date",
        default=DEFAULT_START_DATE,
        help="Start date for backfill (YYYY-MM-DD, defaults to 2020-01-01)",
    )
    parser.add_argument(
        "--output_file",
        default=DEFAULT_OUTPUT_FILE,
        help="Output CSV file (for csv mode, defaults to wx.csv)",
    )
    parser.add_argument(
        "--db_path",
        default=os.environ.get("WEEWX_DB_PATH", DEFAULT_DB_PATH),
        help="Path to WeeWX SQLite database (for db mode, defaults to /data/archive/weewx.sdb)",
    )
    parser.add_argument(
        "--mode",
        choices=['csv', 'db'],
        default='db',
        help="Output mode: 'csv' for CSV file, 'db' for direct SQLite insert (default: db)",
    )

    args = parser.parse_args()

    # Check for required arguments being placeholders
    if args.api_token == "YOUR_API_TOKEN" or args.device_id == "YOUR_DEVICE_ID":
        logging.error(
            "API token and Device ID must be set via command line arguments or environment variables."
        )
        exit(1)  # Exit with an error code

    # Validate database path for db mode
    if args.mode == 'db' and not os.path.exists(args.db_path):
        logging.error(f"Database file not found: {args.db_path}")
        logging.error("Either create the database or use --mode csv")
        exit(1)

    main(args.api_token, args.device_id, args.start_date, args.output_file, args.db_path, args.mode)
