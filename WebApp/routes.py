from flask import Flask, escape, render_template, send_from_directory
import psycopg2
import os.path
from os import path
from geojson import dump
import sys

from flask import jsonify, request
import config

app = Flask(__name__)

buildGeoJSONquery = """
select row_to_json(fc) from (select 'FeatureCollection' as type, array_to_json(array_agg(f)) 
as features from (select 'Feature' as type, st_asgeojson(hg.geom, 6)::json 
as geometry, row_to_json(hp) as properties from hikes as hg inner join (select "Hike_ID", 
"Name" from hikes) as hp on hg."Hike_ID" = hp."Hike_ID") as f) as fc;
"""

# home page
@app.route("/")
def home():
	with psycopg2.connect(dbname=config.DB_NAME, user=config.USER, password=config.PASSWORD,
		host=config.HOST, port=config.PORT) as connection:
		with connection.cursor() as cur:
			cur.execute(buildGeoJSONquery)
			hikeGeoJSON = cur.fetchall()
			with open('static/hikes.geojson', 'w') as f:
				f.write(str(hikeGeoJSON[0][0]).replace("'",'"').replace('$',"'").replace('^',"'"))

	return render_template("home.html")

# return hikes geojson for map
@app.route("/getHikes/hikes.geojson")
def getHikes():
	return send_from_directory("static/", "hikes.geojson")


@app.route("/pickHikeDate/<hikeId>", methods = ['GET', 'POST'])
def pickHikeDate(hikeId):
	if request.method == 'POST':
		hikeJSON = getHikeWeather(hikeId, request.form['date'])
		return render_template("hikeWeather.html", hikeJson = hikeJSON)

	hikeName = getHikeName(hikeId)
	return render_template("pickHikeDate.html", hikeId = hikeId, hikeName = hikeName)


hikeNameQuery ="""select "Name" from hikes where "Hike_ID" = %s;"""

def getHikeName(hikeId):
	with psycopg2.connect(dbname=config.DB_NAME, user=config.USER, password=config.PASSWORD,
							host=config.HOST, port=config.PORT) as connection:
		with connection.cursor() as cur:
			cur.execute(hikeNameQuery, (hikeId,))
			name = cur.fetchone()[0]
	return name


hikeWeatherQuery = """select * from hikeweather where "Hike_ID" = %s and "Month" = %s and "Day" = %s;"""

def weatherRow(x):
	return {'Hike_ID': x[0], 'Month': x[1], 'Day': x[2], 'Stat': x[3], '2019_value': x[4], '2018_value': x[5],
			'3_year_avg': x[6], '5_year_avg': x[7],'10_year_avg': x[8], '20_year_avg': x[9], '30_year_avg': x[10]}


def getHikeWeather(hikeId, date):
	month = int(date[0: 2])
	day = int(date[3: 5])
	with psycopg2.connect(dbname=config.DB_NAME, user=config.USER, password=config.PASSWORD,
							host=config.HOST, port=config.PORT) as connection:
		with connection.cursor() as cur:
			cur.execute(hikeWeatherQuery, (hikeId, month, day))
			hike = list(map(lambda x: weatherRow(x), cur))

	print(hike, file=sys.stdout)
	return jsonify(hike)


if __name__ == "__main__":
	app.run(host='0.0.0.0')