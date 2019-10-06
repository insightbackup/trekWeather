from flask import Flask, escape, render_template, send_from_directory, jsonify, request

import query
import config

app = Flask(__name__)


# home page
@app.route("/")
def home():
	# Create a GeoJSON file that the map uses to display the hikes
	query.createGeoJSON()
	return render_template("home.html", mapToken = config.MAP_TOKEN)

# return hikes geojson for map
@app.route("/getHikes/hikes.geojson")
def getHikes():
	return send_from_directory("static/", "hikes.geojson")

# The main UI interface. If the request method is GET, displays the page to allow the user
# to pick a date for their hike. If the request method is POST, this queries the database and
# then displays the results to the user
@app.route("/pickDate/<cluster>/<hikeId>", methods = ['GET', 'POST'])
def pickDate(cluster, hikeId):
	if request.method == 'POST':
		hikeJSON = query.getHikeWeather(hikeId, request.form['date'])
		return render_template("weather.html", cluster = cluster, hike = hikeJSON.get_json())

	hikeName = query.getHikeName(hikeId)
	return render_template("pickDate.html", cluster = cluster, hikeId = hikeId, hikeName = hikeName)


if __name__ == "__main__":
	app.run(host='0.0.0.0')