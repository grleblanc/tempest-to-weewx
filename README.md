# tempest-to-weewx

A script to backfill historical Weatherflow Tempest to your weewx sqlite3 database.

Markdown

# Tempest to Weewx Backfill Script

This Python script retrieves historical weather data from the [WeatherFlow Tempest API](https://weatherflow.github.io/Tempest/) and exports it to a CSV file in a format that is similar to the Weewx weather station software database schema. This allows you to "backfill" data, or to create your own data logging separate from Weewx.

## Features

- **Fetches Historical Data:** Retrieves data from the Tempest API for a specified time range.
- **Weewx-like CSV Output:** Writes the data to a CSV file with columns similar to the Weewx `archive` table, including unit conversions (metric to US customary).
- **Command-Line Arguments:** Uses `argparse` for flexible configuration via command-line arguments.
- **Environment Variable Support:** Falls back to environment variables for configuration if command-line arguments are not provided.
- **Robust Error Handling:** Includes comprehensive error handling for API requests, JSON parsing, and file I/O, with detailed logging.
- **Retry Mechanism:** Retries API requests on temporary network errors.`
- **Rate Limiting:** Includes a `time.sleep(1)` to avoid overwhelming the Tempest API.
- **Logging:** Uses the Python `logging` module to provide informative output and error messages.
- **Chunking:** Retrieves data in daily chunks to avoid exceeding API limits.

## Requirements

- Python 3.6 or higher
- `requests` library: `pip install requests`

## Usage`

### 1. Install Dependencies

```bash
`pip install requests`
```

### **2\. Run the Script**

You can run the script in several ways:

**A. Using Command-Line Arguments (Recommended):**

Bash

`python tempest_to_csv.py --api_token YOUR_API_TOKEN --station_id YOUR_STATION_ID --start_date YYYY-MM-DD --output_file output.csv [--db_path /path/to/weewx.sdb]`

- \--api_token: Your Tempest API token. **Required.**
- \--station_id: Your Tempest station ID. **Required.**
- \--start_date: The start date for data retrieval (YYYY-MM-DD format). Defaults to 2020-08-07.
- \--output_file: The path to the output CSV file. Defaults to wx.csv.
- \--db_path: The path to your weewx.sdb file. Optional, defaults to /data/weewx/archive/weewx.sdb

**B. Using Environment Variables:**

Set the following environment variables:

- TEMPEST_API_TOKEN: Your Tempest API token.
- TEMPEST_STATION_ID: Your Tempest station ID.
- WEEWX_DB_PATH: (Optional) Path to your weewx database.

Then run the script:

Bash

`python tempest_to_csv.py --start_date YYYY-MM-DD --output_file output.csv`

**C. Using Default Values (Not Recommended):**

If you don't provide command-line arguments or set environment variables, the script will use the default values defined in the DEFAULT\_... constants within the script. **You must edit the script directly to change these defaults.** This is not recommended for regular use, but can be convenient for quick testing.

### **Example**

To backfill data from January 1, 2023, to the present, using an API token of your-api-token and a station ID of your-station-id, and save to my_weather_data.csv, you would run:

Bash

`python tempest_to_csv.py --api_token your-api-token --station_id your-station-id --start_date 2023-01-01 --output_file my_weather_data.csv`

## **Output CSV Format**

The output CSV file will have the following columns, which correspond to commonly used fields in the Weewx database schema:

| Column Name        | Description                                     | Units   |
| :----------------- | :---------------------------------------------- | :------ |
| dateTime           | Unix timestamp (seconds since epoch)            | seconds |
| outTemp            | Outside temperature                             | °F      |
| windSpeed          | Average wind speed                              | mph     |
| windGust           | Wind gust                                       | mph     |
| windDir            | Wind direction                                  | degrees |
| barometer          | Barometric pressure                             | inHg    |
| outHumidity        | Outside humidity                                | %       |
| rain               | Accumulated rainfall                            | inches  |
| UV                 | UV index                                        |         |
| radiation          | Solar radiation                                 | W/m²    |
| lightning_distance | Average lightning strike distance, last 3 hours | miles   |

## **Notes**

- The Tempest API has rate limits. The script includes a 1-second delay between requests to help avoid exceeding these limits.
- The script retrieves data in 1-day chunks. You can adjust the interval variable in the code if you need finer-grained control.
- The script does _not_ automatically detect the units used by your Tempest station. It assumes common default units and performs conversions as needed.
- This script has been refactored to write to a csv. If you'd like to write to a database, you'd need to change the code.

## **Error Handling**

The script includes robust error handling and logging. Errors are logged to the console, including:

- API request errors (e.g., network problems, invalid API token).
- JSON parsing errors.
- File I/O errors.
- Invalid date format errors.

If an error occurs during processing for a particular time range, the script will log the error and continue to the next time range. This ensures that the script doesn't crash entirely if there's a temporary issue.

## **Contributing**

Contributions are welcome\! Please submit pull requests or open issues on the repository.

## **License**

This script is released under the [MIT License](https://www.google.com/url?sa=E&source=gmail&q=LICENSE) (you would need to create a LICENSE file with the MIT license text).
