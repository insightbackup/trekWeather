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
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import Reader._
import Constants._

object TrekWeather {
	def main(args: Array[String]) {

		var startTime = System.currentTimeMillis()

		val runMode = if (args.length == 0) {
			Constants.RUN_ALL
		} else if (args(0).equals("local")) {
			Constants.LOCAL
		} else {
			Constants.SMALL_DATA
		}
		Constants.setConstants(runMode)

		// Set up spark context
		val conf = new SparkConf().setAppName("TrekWeather")
		val sc = new SparkContext(conf)
		val spark = SparkSession.builder.appName("TrekWeather").
							config("spark.master", "local").getOrCreate()
		import spark.implicits._

		// load US weather stations and data into dataframes
//		val stationsDF = Reader.getStations(sc, spark)
		var noaaData = getWeatherStats(sc, spark)
//		noaaData.printSchema
		
		noaaData.show()


		/* Psuedo code for next steps
		 * for (hike in OSM) {
		 *	find K closest stations
		 *	for each station {
		 *		calculate weight by distance
		 *		calculate averages
		 *		store in postgres table
		 *	}
		 *} 
		 */


		 // for (station <- stationsDF.select($"ID")) {
		 // 	println(station(0).getClass())
		 // 	println(noaaData.getClass())
		 // 	startTime = System.currentTimeMillis()
		 // 	var stationData = noaaData.filter($"ID" === station(0))
		 // 	println()
		 // 	println()
		 // 	println("Filtering by station took " + (System.currentTimeMillis() - startTime) 
		 // 		+ " ms")
		 // 	println()
		 // 	println()
		 // }

		// noaaData = noaaData.filter($"Element".isin(desiredElements:_*))
//		years = years.groupBy("ID","Date").pivot("Element",desiredElements).sum("Value")

		// stationsDF.select($"ID").show()
		// noaaData.show()


	} // end main


	/**
	 * This function reads in all the data from the NOAA directory and returns a 
	 * dataframe with the schema:
	 * ID, lat, lon, state, stat, date, 2019-data, 2018-data, 3-year, 3y_count, 5-year,
	 * 5y_count, 10-year, 10y_count, 20-year, 20y_count, all, all_count
	 */
	 def getWeatherStats(sc: SparkContext, spark: SparkSession) = {
	 	// add ability to use dataframes
	 	import spark.implicits._

	 	// get the 2019 data - most recent when created
	 	var years = Reader.getWeatherForYear(sc, spark, 2019)

	 	// start weatherStats with renamed value column
	 	var weatherStats = years.withColumnRenamed("Value", "2019_value")
	 							.drop("Year")

	 	// read in the data, year by year, storing certain information as needed
	 	for (year <- 2018 to 2017 by -1) {
	 		var current = Reader.getWeatherForYear(sc, spark, year)
	 		if (year == 2018) {
			 	weatherStats = weatherStats.join(current, Seq("ID", "Month", "Day", "Stat"),
	 												joinType = "full")
	 								.withColumnRenamed("Value", "2018_value")
	 								.drop("Year")
	 		}
	 		years = years.union(current)

	 		if (year == 2017) {
	 			weatherStats = getAverages("3", weatherStats, years)
	 		} else if (year == 2015) {
	 			weatherStats = getAverages("5", weatherStats, years)
	 		} else if (year == 2010) {
	 			weatherStats = getAverages("10", weatherStats, years)
	 		} else if (year == 2000) {
	 			weatherStats = getAverages("20", weatherStats, years)
	 		} else if (year == 1990) {
	 			weatherStats = getAverages("30", weatherStats, years)
	 		} else if (year == 1970) {
	 			weatherStats = getAverages("50", weatherStats, years)
	 		}
	 	}
//	 	weatherStats = getAverages("all", weatherStats, years)

 	 	// finish by getting the station location data, and joining with weatherStats
 	 	var stationsDF = Reader.getStations(sc, spark)

		weatherStats.join(stationsDF, "ID")

	 } // end getWeatherStats


	 /**
	  * This function counts and averages the data stored in years by station, date,
	  * and measurement statistic then joins it with the weatherStats dataframe. The
	  * string is used to set the newly added column names.
	  */
	  def getAverages(name: String, weatherStats: DataFrame, years: DataFrame) = {
	  	var stats = years.groupBy("ID","Month","Day","Stat")
	  					 .agg(mean("Value"),count("Value"))
	  					 .withColumnRenamed("avg(Value)", name + "_year_avg")
	  					 .withColumnRenamed("count(Value)", name + "_year_count")
	  	weatherStats.join(stats, Seq("ID", "Month", "Day", "Stat"), joinType = "full")
	  }

} // end TrekWeather