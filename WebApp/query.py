# code used by the web app to query the database

from flask import jsonify
import psycopg2
import os.path
from os import path

import config

## Query to return hikes as a GeoJSON string
buildGeoJSONquery = """
select row_to_json(fc) from (select 'FeatureCollection' as type, array_to_json(array_agg(f)) 
as features from (select 'Feature' as type, st_asgeojson(hg.geom, 6)::json 
as geometry, row_to_json(hp) as properties from hikes as hg inner join (select "Hike_ID", 
"Name" from hikes) as hp on hg."Hike_ID" = hp."Hike_ID") as f) as fc;
"""

# Queries database and writes result to a geojson file
def createGeoJSON():
	if (not(path.exists('static/hikes.geojson'))):
		print("File not found so creating")
		with psycopg2.connect(dbname=config.DB_NAME, user=config.USER, password=config.PASSWORD,
			host=config.HOST, port=config.PORT) as connection:
			with connection.cursor() as cur:
				cur.execute(buildGeoJSONquery)
				hikeGeoJSON = cur.fetchall()
				with open('static/hikes.geojson', 'w') as f:
					# need to replace characters that were put in database in place of ' to avoid
					# GeoJSON parsing errors
					f.write(str(hikeGeoJSON[0][0]).replace("'",'"').replace('$',"'"))

stationGeoJSONquery = """
select row_to_json(fc) from (select 'FeatureCollection' as type, array_to_json(array_agg(f)) 
as features from (select 'Feature' as type, st_asgeojson(hg.geom, 6)::json 
as geometry, row_to_json(hp) as properties from stations as hg inner join (select "Station_ID",
"State" from stations) as hp on hg."Station_ID" = hp."Station_ID") as f) as fc;
"""

# Queries database and writes result to a geojson file
def stationGeoJSON():
	if (not(path.exists('static/stations.geojson'))):
		print("File not found so creating")
		with psycopg2.connect(dbname=config.DB_NAME, user=config.USER, password=config.PASSWORD,
			host=config.HOST, port=config.PORT) as connection:
			with connection.cursor() as cur:
				cur.execute(stationGeoJSONquery)
				stations = cur.fetchall()
				with open('static/stations.geojson', 'w') as f:
					# need to replace characters that were put in database in place of ' to avoid
					# GeoJSON parsing errors
					f.write(str(stations[0][0]).replace("'",'"'))


# Query to get name of hike from database
hikeNameQuery ="""select "Name" from hikes where "Hike_ID" = %s;"""

# Given hikeId, returns the name of the hike from the database, with ' as needed
def getHikeName(hikeId):
	with psycopg2.connect(dbname=config.DB_NAME, user=config.USER, password=config.PASSWORD,
							host=config.HOST, port=config.PORT) as connection:
		with connection.cursor() as cur:
			cur.execute(hikeNameQuery, (hikeId,))
			name = cur.fetchone()[0]
	return str(name).replace("'",'"').replace('$',"'")



# Query to pull all weather data for this hike and date
# For current UI, we will only display snowfall, precipitation, and max/min/avg temperature data
hikeWeatherQuery = """
select * from hikeweather where "Hike_ID" = %s and "Month" = %s and "Day" = %s
and ("Stat" = 'SNOW' or "Stat" = 'PRCP' or "Stat" = 'TMAX' or "Stat" = 'TMIN' or "Stat" = 'TAVG');
"""

# for formatting the information display

# checks if entry is null. If it is, returns null; else returns value with appropriate multiplier
# rounded to 2 digits
def roundIfNotNull(x, divideBy=1):
	if x is None:
		return None
	return round(x/divideBy, 2)

# given degrees Celsius, converts to Fahrenheit - add 320 because we are still dealing
# with tenths of degrees
def cToF(x):
	if x is None:
		return None
	return (x * 9.0 / 5.0) + 320

# Snowfall is given in mm
def snowfall(x):
	return {'Stat': "Snowfall (mm)", '2019_value': roundIfNotNull(x[4]), '2018_value': roundIfNotNull(x[5]),
			'3_year_avg': roundIfNotNull(x[6]), '5_year_avg': roundIfNotNull(x[7]),
			'10_year_avg': roundIfNotNull(x[8]), '20_year_avg': roundIfNotNull(x[9]),
			'30_year_avg': roundIfNotNull(x[10])}

# precipitation is given in tenths of mm, so we divide by 10 before returning
def prec(x):
	return {'Stat': "Precipitation (mm)", '2019_value': roundIfNotNull(x[4], 10),
			'2018_value': roundIfNotNull(x[5], 10), '3_year_avg': roundIfNotNull(x[6], 10),
			'5_year_avg': roundIfNotNull(x[7], 10),'10_year_avg': roundIfNotNull(x[8], 10),
			'20_year_avg': roundIfNotNull(x[9], 10), '30_year_avg': roundIfNotNull(x[10], 10)}

# temperature is given in tenths of degrees Celsius; for ease of reading by an American audience,
# we divide by 10 and convert to Fahrenheit before returning
def temp(x):
	tempType = {
		"TMIN": "Min temp (F)",
		"TMAX": "Max temp (F)",
		"TAVG": "Avg temp (F)"		
	}
	return {'Stat': tempType.get(x[3]), '2019_value': roundIfNotNull(cToF(x[4]), 10),
			'2018_value': roundIfNotNull(cToF(x[5]), 10), '3_year_avg': roundIfNotNull(cToF(x[6]), 10),
			'5_year_avg': roundIfNotNull(cToF(x[7]), 10),'10_year_avg': roundIfNotNull(cToF(x[8]), 10),
			'20_year_avg': roundIfNotNull(cToF(x[9]), 10), '30_year_avg': roundIfNotNull(cToF(x[10]), 10)}


# Given a row of the query output, returns a list that will create a JSON with proper format
def weatherRow(x):
	statType = {
		"SNOW": snowfall,
		"PRCP": prec,
		"TMAX": temp,
		"TMIN": temp,
		"TAVG": temp
	}
	stat = statType.get(x[3], "SKIP")
	if (stat == "SKIP"):
		return {}

	return stat(x)


# Given a hikeId and date, returns a JSON-type response that holds the information about the 
# weather for this hike
def getHikeWeather(hikeId, date):
	month = int(date[0: 2])
	day = int(date[3: 5])
	name = getHikeName(hikeId)
	with psycopg2.connect(dbname=config.DB_NAME, user=config.USER, password=config.PASSWORD,
							host=config.HOST, port=config.PORT) as connection:
		with connection.cursor() as cur:
			cur.execute(hikeWeatherQuery, (hikeId, month, day))
			weatherStats = list(map(lambda x: weatherRow(x), cur))

	return jsonify({
				'Hike_ID': hikeId,
				'Name': name,
				'Month': month,
				'Day': day,
				'properties': weatherStats
				})
