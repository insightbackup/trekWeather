#!bin/bash

# add geom column to table
alter table noaa add column geom geometry(Point, 4326);

# calculate the geometry points for use with PostGIS queries
update noaa set geom = ST_SetSRID(ST_point("Lat","Lon"), 4326);