# Big Data

## Overview

This project provides a comprehensive data pipeline and interactive dashboard for analyzing stock market data. The data pipeline processes raw data, cleans it, and stores it in a TimescaleDB database. The dashboard visualizes the processed data, allowing users to explore market trends and statistics interactively.

## Requirements

Ensure the following requirements are installed:

- Docker
- Docker Compose
- Python packages:
  - `psycopg2-binary`
  - `sqlalchemy`
  - `sqlalchemy-timescaledb`
  - `numpy`
  - `pandas`
  - `scikit-learn`

The Python packages should be installed by the docker-compose.yml once launched.

## How to Launch the Project

### Step 1: Download and Place Data
1. Create `bourse/data` directory.
2. Download the required data and place the `boursorama` folder inside the `bourse/data` directory.

### Step 2: Configure Paths

Edit the `docker-compose.yml` file to set the correct paths:

```yaml
volumes:
  - /path/to/data:/home/bourse/data/
  - /path/to/timescaledb:/var/lib/postgresql/data/timescaledb
```

- Replace `/path/to/data` with the path to your raw data directory.
- Replace `/path/to/timescaledb` with the path for your database storage.

### Step 3 : Connect with docker login
1. Create an account on https://hub.docker.com/  
2. Launch the following command and log into your account
```shell
docker login
```

### Step 4: Launch the Docker Container
Navigate to the `docker/` directory and use the following command to start the process:

```bash
docker-compose down; cd analyzer/; make; cd ../dashboard/; make; cd ..; docker-compose up;
```

### Step 5: Wait for Processing
The process takes approximately between 3hours 30 minutes and 4hours to complete. You will see "Done" at the end of the process.

### Step 6: Access the Dashboard
Once the process is complete, go to `localhost:8050` in your web browser to see the dashboard.

## Main Scripts

### analyzer.py

The analyzer script processes the raw stock data, cleans it, and stores it in the database. Key steps include:

- Separating each company using its symbol and associating the company with a market.
- Removing rows with `volume == 0` as they indicate no change from the last recorded value.
- Using the `tags` table to track the number of companies associated with each market.
- Processing and writing data to the database month by month, in batches.

### bourse.py

The `bourse.py` script powers the dashboard. Key functionalities include:

- Updating the dropdown of markets using a button.
- Selecting multiple companies from a chosen market and viewing them on a graph.
- Displaying data in log, linear, or candlestick formats.
- Showing Bollinger Bands with configurable windows.
- Choosing the frequency for data display on the graph.
- Selecting a company to display daily data in a table.

### Additional Note

- A sleep command is included in the analyzer Dockerfile to ensure the database is created before launching the analyzer script.

## Conclusion

This project demonstrates the creation of a data pipeline and dashboard for stock market data analysis. By following the steps outlined, you can process raw data, store it in a TimescaleDB database, and visualize it using an interactive dashboard.

## Authors

- Lina Farchado - SCIA
- Florian Tigoulet - SCIA
