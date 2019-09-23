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
		
//		noaaData.show()

		// specifying the desired measurements
		val desiredElements = Seq("PRCP","SNOW","SNWD","TAVG","TMAX","TMIN","TOBS",
								  "WT01","WT02","WT03","WT04","WT05","WT06","WT07",
								  "WT08","WT09","WT10","WT11","WT12","WT13","WT14",
								  "WT15","WT16","WT17","WT18","WT19","WT20","WT21","WT22")

		/* Psuedo code for next steps
		 * 
		 * for (station in stationsDF(0)) {
		 *	var stationData = noaaData.filter($"ID" == station)
		 *	for (date in year) {
		 *		record most recent data (2019 or 2018)
		 *		get 3-year, 5-year, 10-year, 20-year, all-year count and averages for each stat
		 *		write into appropriate postGIS table
		 *	}
		 *}
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

	 	// start with the station data, which contains location
	 	var weatherStats = Reader.getStations(sc, spark)

	 	// now get the 2019 data - most recent when created
	 	var years = Reader.getWeatherForYear(sc, spark, 2019)

	 	// join the 2019 data to weatherStats and rename value column
	 	weatherStats = weatherStats.join(years, "ID")
	 							   .withColumnRenamed("Value", "2019_value")
	 							   .drop("Year")

	 	// attempt to get 2018 data, join and see what happens

	 	// read in the data, year by year, storing certain information as needed
	 	for (year <- 2018 to 1763 by -1) {
	 		var current = Reader.getWeatherForYear(sc, spark, year)
	 		if (year == 2018) {
			 	weatherStats = weatherStats.join(current, Seq("ID", "Month", "Day", "STAT"),
	 												joinType = "full")
	 								.withColumnRenamed("Value", "2018_value")
	 								.drop("Year")
	 		}
	 		years = years.union(current)

	 		if (year == 2017) {
	 			weatherStats = getAverages(3, years)
	 		} else if (year == 2015) {
	 			weatherStats = getAverages(5, years)
	 		} else if (year == 2010) {
	 			weatherStats = getAverages(10, years)
	 		} else if (year == 2000) {
	 			weatherStats = getAverages(20, years)
	 		}
	 	}
	 } // end getWeatherStats

} // end TrekWeather