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
				.load("s3a://kimport-de-data/noaa/by_year/*.csv")

		years = years.toDF(Seq("ID","Date","Element","Value","M","Q","S","OBS-TIME"): _*)
		years = years.filter($"ID".startsWith("US"))
		years = years.withColumn("Value",years.col("Value").cast("int"))

		val desiredElements = Seq("PRCP","SNOW","SNWD","TAVG","TMAX","TMIN","TOBS",
								  "WT01","WT02","WT03","WT04","WT05","WT06","WT07",
								  "WT08","WT09","WT10","WT11","WT12","WT13","WT14",
								  "WT15","WT16","WT17","WT18","WT19","WT20","WT21","WT22")
		years = years.groupBy("ID","Date").pivot("Element",desiredElements).sum("Value")

		years.show()



	}
}