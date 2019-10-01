#  Trek Weather

## Project description

When it comes to planning a hike, weather conditions can influence the supplies and equipment you need. Trek Weather allows the user to view weather statistics (average/high/low temp, preciptation, snowfall) by route.

## The data

Data is compiled from NOAA's Global Historical Climate Network (https://docs.opendata.aws/noaa-ghcn-pds/readme.html) and OpenStreetMap (https://docs.opendata.aws/osm-pds/readme.html).

## Tech Stack

Data will be processed and merged via Spark and then stored in a database (tech TBD) for easy search. Flask will be used to provide a search and display UI.

## Engineering challenge

1. Interpolate and store weather data for locations "far" from a weather station.

1. Merge weather data with hiking routes.

1. Balance the storage requirement for pre-processing all statistics with desire for low latency when displaying information.

## End product and MVP

Ideal end product will allow for search by route, ability to zoom in on a portion of the route, and also filter by time of year/specific date. A minimum viable product would provide this ability routes within a specific state.

