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
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import java.time.LocalDate

import Reader._
import Constants._

object OsmProcess {

	// For processing the OpenStreetMap data, we load the data and then query
	// the PostGIS database of weather data to determine the statistics for each hike
	def main(args: Array[String]) {

		// Set up spark context
		val conf = new SparkConf().setAppName("OSM-Process")
		val sc = new SparkContext(conf)
		val spark = SparkSession.builder.appName("OSM-Process").
							config("spark.master", "local").getOrCreate()
		import spark.implicits._

		// create dataframe that contains hike names and locations
		val hikes = getHikesList(sc, spark)

		// write hikes to database for querying from UI
		hikes.write.format("jdbc").option("url", Constants.DB_URL)
				.option("dbtable","hikes").option("user", Constants.DB_USER)
				.option("password", Constants.DB_PASSWORD)
				.option("driver", "org.postgresql.Driver").mode("overwrite").save()

		// map each hike to weather data for each day of the year
		mapHikesToWeather(hikes, sc, spark)

//		weatherStats.show()



	} // end main

	// Read in data from OpenStreetMap stored in S3 and obtain the names and rough
	// GPS coordinates of hikes
	def getHikesList(sc: SparkContext, spark: SparkSession) = {
		// create schema for reading in hike data
	 	val hikeSchema = StructType(
	 		List(
	 			StructField("ID", DoubleType, false),
	 			StructField("Lat", DoubleType, false),
	 			StructField("Lon", DoubleType, false),
	 			StructField("Name", StringType, false),
	 			StructField("Highway", StringType, true),
	 			StructField("Route", StringType, true),
	 			StructField("Sac_Scale", StringType, true)
	 		)
	 	)


		val osmData = spark.read.format("csv").option("header", "true")
							.schema(hikeSchema)
							.load(Constants.OSM_DATA_DIR + "west_hike.csv")

		osmData.filter(col("ID") > scala.math.pow(10, 15))
			   .filter(col("Name").isNotNull)

	} // end getHikesList

	// attempt at writing udf...
	// val getAverages = udf ((lat: Double, lon: Double, id: Double) => 
	// 	averageWeather(lat, lon, id))


	// The main work of this process, this functions takes the hikes dataframe and
	// returns a dataframe with one row per hike per day with aggregate weather data
	// for that hike, taken as a weighted average of nearby stations
	def mapHikesToWeather(hikes: DataFrame, sc: SparkContext, spark: SparkSession) = {
		import spark.implicits._

//		val statNames = sc.parallelize(Seq("TMAX", "TMIN", "PRCP", "SNOW", "TAVG"))
		
//		statNames.flatMap(name => makeTables(name, hikes, sc, spark))
		
		// hikes.join(getAverages(hikes.col("Lat"), hikes.col("Lon"), hikes.col("ID"), spark))


	} // end mapHikesToWeather


	def makeTables(name: String, hikes: DataFrame, sc: SparkContext, spark: SparkSession) = {

		import spark.implicits._


	}


	// def testFunction (lat: Column, lon: Column, id: Column, spark: SparkSession) : DataFrame = {

	// 	import spark.implicits._

	// 	val closeStations = "(select noaa.*, stations.\"Lat\", stations.\"Lon\", stations.\"State\", " +
	// 						"stations.geom from noaa, stations where st_dwithin(geom, st_makepoint(" + 
	// 						lat + ", " + lon + ")::geography, 50000) and noaa.\"ID\" = " +
	// 						"stations.\"ID\") as foo"

	// 	// retrieve information from nearby stations
	// 	val weather = spark.read.format("jdbc").option("url", Constants.WEATHER_URL)
	// 			.option("dbtable", closeStations).option("user", Constants.DB_USER)
	// 			.option("password", Constants.DB_PASSWORD)
	// 			.option("driver", "org.postgresql.Driver").load()


	// 	weather.withColumn("ID",id)

	// }


	// Given latitude and longitude, returns the approximated weather for that hike
//	def averageWeather(hikeLat: Double, hikeLon: Double, sc: SparkContext, spark: SparkSession) = {
	def averageWeather(hikeLat: Double, hikeLon: Double, id: Double, spark: SparkSession): DataFrame = {

		import spark.implicits._

		// query database for all stations within 50km and return all weather data associated
		// with those stations
		val closeStations = "(select noaa.*, stations.\"Lat\", stations.\"Lon\", stations.\"State\", " +
							"stations.geom from noaa, stations where st_dwithin(geom, st_makepoint(" + 
							hikeLon + ", " + hikeLat + ")::geography, 50000) and noaa.\"ID\" = " +
							"stations.\"ID\") as foo"
		val weather = spark.read.format("jdbc").option("url", Constants.DB_URL)
				.option("dbtable", closeStations).option("user", Constants.DB_USER)
				.option("password", Constants.DB_PASSWORD)
				.option("driver", "org.postgresql.Driver").load()


		// compute the average weather data for the given hike
		val averages = weather.withColumn("3_year_tot", $"3_year_avg" * $"3_year_count")
						.withColumn("5_year_tot", $"5_year_avg" * $"5_year_count")
						.withColumn("10_year_tot", $"10_year_avg" * $"10_year_count")
						.withColumn("20_year_tot", $"20_year_avg" * $"20_year_count")
						.withColumn("30_year_tot", $"30_year_avg" * $"30_year_count")
						.groupBy("Month", "Day", "Stat")
						.agg(sum("3_year_tot").alias("3_year_num"), sum("3_year_count").alias("3_year_denom"),
							sum("5_year_tot").alias("5_year_num"), sum("5_year_count").alias("5_year_denom"),
							sum("10_year_tot").alias("10_year_num"), sum("10_year_count").alias("10_year_denom"),
							sum("20_year_tot").alias("20_year_num"), sum("20_year_count").alias("20_year_denom"),
							sum("30_year_tot").alias("30_year_num"), sum("30_year_count").alias("30_year_denom"))
						.withColumn("3_year_avg", $"3_year_num" / $"3_year_denom")
						.withColumn("5_year_avg", $"5_year_num" / $"5_year_denom")
						.withColumn("10_year_avg", $"10_year_num" / $"10_year_denom")
						.withColumn("20_year_avg", $"20_year_num" / $"20_year_denom")
						.withColumn("30_year_avg", $"30_year_num" / $"30_year_denom")
						.drop("3_year_num", "3_year_denom", "5_year_num", "5_year_denom", 
							  "10_year_num", "10_year_denom", "20_year_num", "20_year_denom",
							  "30_year_num", "30_year_denom")

		averages.withColumn("ID", lit(id))

	} // end averageWeather


} // end OsmProcess