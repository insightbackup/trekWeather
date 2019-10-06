/** NoaaProcess
 *
 * This object contains the methods for loading weather data from NOAA's Global 
 * Historical Climate Network. Data is read in year by year, with the desired
 * averages and counts noted as files are read.
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import Constants._
import Reader._

object NoaaProcess {

	// This function reads in all the data from the NOAA directory and returns a 
	// dataframe with the schema:
	// ID, lat, lon, state, stat, date, 2019-data, 2018-data, then the average and
	// number of data points for the relevant groupings of last 3, 5, 10, 20, and 30 years.
	def getWeatherStats(spark: SparkSession) = {
	 	// add ability to use dataframes
	 	import spark.implicits._

	 	// get the 2019 data - most recent when created
	 	var years = Reader.getWeatherForYear(spark, 2019)

	 	// start weatherStats with renamed value column
	 	var weatherStats = years.withColumnRenamed("Value", "2019_value").drop("Year")

	 	// read in the data, year by year, storing certain information as needed
	 	// For this application, we only go back 30 years
	 	for (year <- 2018 to 1990 by -1) {
	 		var current = Reader.getWeatherForYear(spark, year)
	 		if (year == 2018) {
			 	weatherStats = weatherStats
			 			.join(current, Seq("Station_ID", "Month", "Day", "Stat"), joinType = "full")
	 					.withColumnRenamed("Value", "2018_value").drop("Year")
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
	 	// In case an application wants the average for all years loaded, uncomment this line
	 	//weatherStats = getAverages("all", weatherStats, years)

		weatherStats
	} // end getWeatherStats


	
	// This function counts and averages the data stored in years by station, date,
	// and measurement statistic then joins it with the weatherStats dataframe. The
	// string is used to set the newly added column names.
	def getAverages(name: String, weatherStats: DataFrame, years: DataFrame) = {
	 	var stats = years.groupBy("Station_ID","Month","Day","Stat")
	  					 .agg(mean("Value"),count("Value"))
	  					 .withColumnRenamed("avg(Value)", name + "_year_avg")
	  					 .withColumnRenamed("count(Value)", name + "_year_count")
	  	weatherStats.join(stats, Seq("Station_ID", "Month", "Day", "Stat"), joinType = "full")
	} // end getAverages

} // end NoaaProcess