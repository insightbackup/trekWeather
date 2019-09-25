/**
 *
 * OpenStreetMap data processing
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import Reader._
import Constants._

object OsmProcess {

	
	def main(args: Array[String]) {

		// Set up spark context
		val conf = new SparkConf().setAppName("NoaaProcess")
		val sc = new SparkContext(conf)
		val spark = SparkSession.builder.appName("NoaaProcess").
							config("spark.master", "local").getOrCreate()
		import spark.implicits._

		// create dataframe that contains hike names and locations
		val hikes = getHikesList(sc, spark)

		// map each hike to weather data for each day of the year
		val weatherStats = mapHikesToWeather(hikes, spark)

		// write weatherStats to database for querying from UI
		weatherStats.write.format("jdbc").option("url", Constants.WEATHER_URL)
				.option("dbtable","weather_stats").option("user", Constants.DB_USER)
				.option("password", Constants.DB_PASSWORD)
				.option("driver", "org.postgresql.Driver").mode("overwrite").save()

	} // end main


} // end OsmProcess