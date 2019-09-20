/** TrekWeather
 * 
 * Project for Insight Data Engineering, Fall 2019.
 * 
 * Author: Sam Kimport
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._

object TrekWeather {
	def main(args: Array[String]) {

		// Set up spark context
		val conf = new SparkConf().setAppName("TrekWeather")
		val sc = new SparkContext(conf)
		val spark = SparkSession.builder.appName("TrekWeather").
							config("spark.master", "local").getOrCreate()
		import spark.implicits._

		// load file from s3
		val stations = sc.textFile("s3a://kimport-de-data/noaa/ghcnd-stations.txt")

		// split into pieces: ID, lat, lon, state
		val splitLines = stations.map(line => (line.substring(0,11), 
		      line.substring(12,20), line.substring(21,30), line.substring(38,40)))

		// filter by stations in the US
		val usStations = splitLines.filter(x => x._1.substring(0,2) == "US")

		val stationsDF = usStations.toDF("ID", "Lat", "Lon", "State")

		var years = spark.read.format("csv").option("header","false")
				.load("s3a://kimport-de-data/noaa/by-year/*.csv")

		years.show()

	}
}