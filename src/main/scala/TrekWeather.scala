/** TrekWeather
 * 
 * Project for Insight Data Engineering, Fall 2019.
 * 
 * Author: Sam Kimport
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TrekWeather {
	def main(args: Array[String]) {

		// Set up spark context
		val conf = new SparkConf().setAppName("TrekWeather")
		val sc = new SparkContext(conf)

		// load file from s3
		val stations = sc.textFile("s3a://kimport-de-data/noaa/ghcnd-stations.txt")

		// split into pieces: ID, lat, lon, state
		val splitLines = stations.map(line => Array(line.substring(0,11), 
		      line.substring(12,20), line.substring(21,30), line.substring(38,40)))

		// filter by stations in the US
		val usStations = splitLines.filter(x => x(0).substring(0,2) == "US")

		// filter further by for only lower 48
		val lower48 = usStations.filter(x => (x(3) != "AK" && x(3) != "HI"))

		// confirm by printing first 10
		lower48.take(10).foreach(x => println(x.deep.mkString(" ")))
	}
}