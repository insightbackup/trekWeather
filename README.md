#  Trek Weather

## Project description

Planning a hike can take some planning. Whether trip is a day, a week, or a month, picking the time of year and securing the proper supplies often needs to be done before weather forecasts would be helpful. Trek Weather allows the user to view weather statistics (average/high/low temp, preciptation, snowfall) by route.

## The data

Data is compiled from NOAA's Global Historical Climate Network (https://docs.opendata.aws/noaa-ghcn-pds/readme.html) and OpenStreetMap (https://docs.opendata.aws/osm-pds/readme.html).

## Tech Stack

Data will be processed and merged via Spark and then stored in a database (tech TBD) for easy search. Flask will be used to provide a search and display UI.

## Engineering challenge

1. Interpolate and store weather data for locations "far" from a weather station.

1. Merge weather data with hiking and biking routes.

1. Determine an effective way to split longer biking and hiking routes to supply more accurate weather data.

1. Balance the storage requirement for pre-processing all statistics with desire for low latency when displaying information.

## End product and MVP

Ideal end product will allow for search by route, ability to zoom in on a portion of the route, and also filter by time of year/specific date. A minimum viable product would provide this ability routes within a specific state.

