#!/bin/bash
# download, clean, and push OpenStreetMap hiking data to S3

# install osmfilter and osmconvert first - sudo apt install osmctools

# midwest data
wget http://download.geofabrik.de/north-america/us-midwest-latest.osm.pbf
osmconvert us-midwest-latest.osm.pbf -o=midwest.o5m

# filter
osmfilter midwest.o5m --keep="highway=track and name=" -o=midwest_track.osm
osmfilter midwest.o5m --keep="route=hiking and name=" -o=midwest_hike.osm
osmfilter midwest.o5m --keep="sac_scale= and name=" -o=midwest_sac.osm
osmfilter midwest.o5m --keep="highway=path and name=" -o=midwest_path.osm

# convert to csv
osmconvert midwest_track.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=midwest_track.csv
osmconvert midwest_hike.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=midwest_hike.csv
osmconvert midwest_sac.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=midwest_sac.csv
osmconvert midwest_path.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=midwest_path.csv

# copy to s3
aws s3 cp midwest_track.csv s3://kimport-de-data/osm/midwest_track.csv
aws s3 cp midwest_hike.csv s3://kimport-de-data/osm/midwest_hike.csv
aws s3 cp midwest_sac.csv s3://kimport-de-data/osm/midwest_sac.csv
aws s3 cp midwest_path.csv s3://kimport-de-data/osm/midwest_path.csv






# northeast data
wget http://download.geofabrik.de/north-america/us-northeast-latest.osm.pbf
osmconvert us-northeast-latest.osm.pbf -o=northeast.o5m

# filter
osmfilter northeast.o5m --keep="highway=track and name=" -o=northeast_track.osm
osmfilter northeast.o5m --keep="route=hiking and name=" -o=northeast_hike.osm
osmfilter northeast.o5m --keep="sac_scale= and name=" -o=northeast_sac.osm
osmfilter northeast.o5m --keep="highway=path and name=" -o=northeast_path.osm

# convert to csv
osmconvert northeast_track.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=northeast_track.csv
osmconvert northeast_hike.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=northeast_hike.csv
osmconvert northeast_sac.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=northeast_sac.csv
osmconvert northeast_path.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=northeast_path.csv

# copy to s3
aws s3 cp northeast_track.csv s3://kimport-de-data/osm/northeast_track.csv
aws s3 cp northeast_hike.csv s3://kimport-de-data/osm/northeast_hike.csv
aws s3 cp northeast_sac.csv s3://kimport-de-data/osm/northeast_sac.csv
aws s3 cp northeast_path.csv s3://kimport-de-data/osm/northeast_path.csv





# south data
wget http://download.geofabrik.de/north-america/us-south-latest.osm.pbf
osmconvert us-south-latest.osm.pbf -o=south.o5m

# filter
osmfilter south.o5m --keep="highway=track and name=" -o=south_track.osm
osmfilter south.o5m --keep="route=hiking and name=" -o=south_hike.osm
osmfilter south.o5m --keep="sac_scale= and name=" -o=south_sac.osm
osmfilter south.o5m --keep="highway=path and name=" -o=south_path.osm

# convert to csv
osmconvert south_track.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=south_track.csv
osmconvert south_hike.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=south_hike.csv
osmconvert south_sac.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=south_sac.csv
osmconvert south_path.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=south_path.csv

# copy to s3
aws s3 cp south_track.csv s3://kimport-de-data/osm/south_track.csv
aws s3 cp south_hike.csv s3://kimport-de-data/osm/south_hike.csv
aws s3 cp south_sac.csv s3://kimport-de-data/osm/south_sac.csv
aws s3 cp south_path.csv s3://kimport-de-data/osm/south_path.csv





# west data
wget http://download.geofabrik.de/north-america/us-west-latest.osm.pbf
osmconvert us-west-latest.osm.pbf -o=west.o5m

# filter
osmfilter west.o5m --keep="highway=track and name=" -o=west_track.osm
osmfilter west.o5m --keep="route=hiking and name=" -o=west_hike.osm
osmfilter west.o5m --keep="sac_scale= and name=" -o=west_sac.osm
osmfilter west.o5m --keep="highway=path and name=" -o=west_path.osm

# convert to csv
osmconvert west_track.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=west_track.csv
osmconvert west_hike.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=west_hike.csv
osmconvert west_sac.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=west_sac.csv
osmconvert west_path.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=west_path.csv

# copy to s3
aws s3 cp west_track.csv s3://kimport-de-data/osm/west_track.csv
aws s3 cp west_hike.csv s3://kimport-de-data/osm/west_hike.csv
aws s3 cp west_sac.csv s3://kimport-de-data/osm/west_sac.csv
aws s3 cp west_path.csv s3://kimport-de-data/osm/west_path.csv





# pacific data
wget http://download.geofabrik.de/north-america/us-pacific-latest.osm.pbf
osmconvert us-pacific-latest.osm.pbf -o=pacific.o5m

# filter
osmfilter pacific.o5m --keep="highway=track and name=" -o=pacific_track.osm
osmfilter pacific.o5m --keep="route=hiking and name=" -o=pacific_hike.osm
osmfilter pacific.o5m --keep="sac_scale= and name=" -o=pacific_sac.osm
osmfilter pacific.o5m --keep="highway=path and name=" -o=pacific_path.osm

# convert to csv
osmconvert pacific_track.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=pacific_track.csv
osmconvert pacific_hike.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=pacific_hike.csv
osmconvert pacific_sac.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=pacific_sac.csv
osmconvert pacific_path.osm --all-to-nodes --csv="@id @lat @lon name highway route sac_scale" --csv-headline --csv-separator="," -o=pacific_path.csv

# copy to s3
aws s3 cp pacific_track.csv s3://kimport-de-data/osm/pacific_track.csv
aws s3 cp pacific_hike.csv s3://kimport-de-data/osm/pacific_hike.csv
aws s3 cp pacific_sac.csv s3://kimport-de-data/osm/pacific_sac.csv
aws s3 cp pacific_path.csv s3://kimport-de-data/osm/pacific_path.csv


