#  Web interface

This web interface allows the user to query the database for summary weather statistics for a given hike on a given day.

This app is created in Flask, using [MapBox](https://www.mapbox.com) for the map and [Bootstrap](https://getbootstrap.com) for much of the other features.

The app queries the PostgreSQL database created by the Batch processing job. The details for accessing this database are stored in the (not included) config.py file.
