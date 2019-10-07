#  Batch processing

This directory contains the source code for the batch processing of data used in Trek Weather.

## Setup instructions

To run this code on a Spark cluster, there are two initial steps:

1. Create a file named Constants.scala in src/main/scala. This file should contain:

	* NOAA_STATIONS, NOAA_DATA_DIR, and OSM_DATA_DIR which contain strings that point to the locations of ghcnd-stations.txt, the csv files sorted by year from NOAA, and the csv files containing the hikes from OpenStreetMap.

	* DB_USER, DB_PASSWORD, and DB_URL used to allow Spark to write to a PostgreSQL database with the PostGIS extension.

1. Run sbt package to compile all source code into a jar to be used with spark-submit to initiate the job.

## Overview of the process

### Step 1: Load stations and hikes into PostGIS

After loading the data from S3 and writing to the database, we create a geometry column using the Lat/Lon pairs and an index for that column for efficient spacial search. The code for this is in the Reader file.

Note: The hike IDs are converted from a 16-digit double into a hex string before the table is stored since PostgreSQL only has 15 digits of precision for doubles.

### Step 2: Load weather data into Spark

The second step, in the NoaaProcess file, reads in the yearly data. It filters for certain measurements (like snowfall, precipitation, and temperature readings) and calculates the following averages:

* 2017-2019

* 2015-2019

* 2010-2019

* 2000-2019

* 1990-2019

The NOAA data is somewhat sparse, meaning not all stations take all measurements each day. For this reason, we also store a count of how many data points are in each average, which is used when calculating the weather by hike in the next step.

### Step 3: Match hikes with nearby stations

The main bottleneck in the process is matching hikes with neaby stations. For this step, the hikes are collected on the master node and the database is queried hike-by-hike to find all weather stations within 50km.

### Step 4: Calculate hike weather

Once the matching is done, the "hike weather" is calculated as an average of the weather measurements from the nearby stations. The results are then written to PostgreSQL for use by the web interface.