# Snowfall and Weather Data Pipeline

This repository contains a modular ETL pipeline designed for Databricks to collect, process, and aggregate snow observations and weather forecasts. The system utilizes a Medallion Architecture to manage data flow from raw API ingestion to business-ready analytical tables.

---

## Project Structure

The project is organized into modules to separate configuration, core logic, and orchestration, following standard software engineering practices for Databricks Git Folders.

* **config/**
    * `settings.py`: Contains global constants, including API User Agents, default states (e.g., Colorado), and Delta table paths.
* **modules/**
    * `clients.py`: Houses the `SnotelClient` and `NWSClient` classes responsible for interacting with USDA and National Weather Service APIs.
    * `database.py`: Contains utility functions for Spark session management, watermark detection for incremental loading, and Delta Lake Merge (upsert) logic.
    * `transformer.py`: Handles data cleaning for historical observations and Spark-based aggregation of forecast data.
* **notebooks/**
    * `01_station_refresh.py`: A notebook dedicated to updating the inventory of weather stations and SNOTEL sites.
    * `02_etl_workflow.py`: The main orchestration notebook that handles full refreshes or incremental updates based on user parameters.

---

## Core Components

### Data Sources
The pipeline integrates data from two primary sources:
1.  **SNOTEL (USDA)**: Provides historical snow depth and Snow Water Equivalent (SWE) data from mountain weather stations.
2.  **National Weather Service (NWS)**: Provides hourly weather forecasts and specific snowfall grid data based on station coordinates.

### Medallion Architecture
The pipeline moves data through logical stages to ensure data quality and performance:
* **Bronze Layer**: Stores raw API responses for weather stations, snow observations, and forecasts.
* **Gold Layer**: Stores the combined forecast table, which joins hourly weather data with snowfall grid intervals using Spark SQL to ensure high performance.



---

## Implementation Details

### Incremental Updates
The pipeline supports two modes of operation controlled via Databricks widgets:
* **Full Refresh**: Overwrites existing tables with a complete history from a defined start date.
* **Incremental**: Identifies the most recent record in the Delta table using a watermark function. It then fetches data from that timestamp until the current date plus one day to ensure no gaps in the time series.

### Efficient Aggregation
To optimize performance, the aggregation logic avoids redundant API calls. It reads the raw weather and snow forecast tables already saved in the Bronze layer and performs a conditional join using Spark. This ensures that weather conditions (temperature, precipitation probability) are accurately mapped to the specific time windows defined by the snowfall grid forecast without hitting external API rate limits.

---

## Setup and Deployment

1.  **Databricks Repos**: Clone this GitHub repository directly into a Databricks Git Folder.
2.  **Library Dependencies**: Ensure `lxml` is installed in your cluster environment to support Pandas HTML parsing.
3.  **Tables**: Ensure the databases and schemas referenced in `config/settings.py` exist within your Databricks environment.
4.  **Workflow**: Schedule `02_etl_workflow.py` as a Databricks Job. Set the `mode` parameter to `incremental` for automated daily runs.
