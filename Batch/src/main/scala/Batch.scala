/** Batch data processing for Trek Weather
 *
 * This object handles the data processing for the Trek Weather user interface.
 * It reads NOAA and OpenStreetMap data into dataframes, carries out the necessary joins
 * and computations, then writes the results to the database.
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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import java.sql.DriverManager
import java.sql.Connection

import Constants._

object Batch {

	def main(args: Array[String]) {

		// Set up spark context
		val conf = new SparkConf().setAppName("Batch")
		val sc = new SparkContext(conf)
		val spark = SparkSession.builder.appName("Batch").
							config("spark.master", "local").getOrCreate()
		import spark.implicits._

		// get weather station information and write it to database
	  	createStationsTable(sc, spark)

	  	// get hike information and write it to database
	  	val hikes = createHikesTable(spark)

		// load averaged US weather data into dataframe
		val noaaData = getWeatherStats(spark)

		val startTime = System.currentTimeMillis()

		// associate each hike to the stations close to it
		val hikesDF = mapHikesToStations(hikes)

		println("\n\n\n\n Time taken = " + (System.currentTimeMillis() - startTime) + "\n\n\n\n")

		// join the weather stats to the hikes
		val hikesWithWeather = hikesDF.join(noaaData, "Station_ID")

		// repartition the dataframe to rebalance the size after the column 
		// explosion and join. Then compute the weather averages for each hike
		// and write result to database
		storeWeatherAverages(hikesWithWeather.repartition($"Hike_ID"))

	} // end main


	// This method handles the creation of the NOAA stations table in PostGIS,
	// including the addition and computation of the geometry column. This will be used
	// to find the closest stations to each hike.
	def createStationsTable (sc: SparkContext, spark: SparkSession) {
		import spark.implicits._

		// Create stations dataframe
		val stations = sc.textFile(Constants.NOAA_STATIONS)

		// split into pieces: ID, lat, lon, state
		val splitLines = stations.map(line => (line.substring(0,11), 
		      line.substring(12,20), line.substring(21,30), line.substring(38,40)))

		// filter by stations in the US
		val usStations = splitLines.filter(x => x._1.substring(0,2) == "US")

		val stationsDF = usStations.toDF("Station_ID", "Lat", "Lon", "State")
				.withColumn("Lat", $"Lat".cast("double"))
				.withColumn("Lon", $"Lon".cast("double"))

		// write stations to database
	  	stationsDF.write.format("jdbc").option("url", Constants.DB_URL)
				.option("dbtable","stations").option("user", Constants.DB_USER)
				.option("password", Constants.DB_PASSWORD)
				.option("driver", "org.postgresql.Driver").mode("overwrite").save()

		addGeometryCol("stations")
	} // end createStationsTable

	// This method handles the creation of the hikes table in PostGIS, including
	// the addition and computation of the geometry column. This will be used by the UI.
	def createHikesTable (spark: SparkSession) = {

		// create schema and read in hike data
	 	val hikeSchema = StructType(
	 		List(
	 			StructField("Hike_ID", DoubleType, false),
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
							.load(Constants.OSM_DATA_DIR + "*_hike.csv")

		// filter for hikes that have names and then remove ' and " from hike names
		// for use in JSON with Flask
		val hikes = osmData.filter(col("Hike_ID") > scala.math.pow(10, 15))
						   .withColumn("Hike_ID", hex(floor(col("Hike_ID"))))
						   .filter(col("Name").isNotNull)
						   .withColumn("Name", regexp_replace(col("Name"), "\'", "\\$"))
						   .withColumn("Name", regexp_replace(col("Name"), "\"", "\\$"))
						   .dropDuplicates("Lat", "Lon")
						   .drop("Highway","Route","Sac_Scale")

		// write hikes to database for querying from UI
		hikes.write.format("jdbc").option("url", Constants.DB_URL)
				.option("dbtable","hikes").option("user", Constants.DB_USER)
				.option("password", Constants.DB_PASSWORD)
				.option("driver", "org.postgresql.Driver").mode("overwrite").save()

		addGeometryCol("hikes")

		//

		// return hike
		hikes
	} // end createHikesTable

	// Given a table name in the PostGIS database, adds a geometry column
	// Assumes table has a table titled "Lat" and one titled "Lon" for latitude/longitude respectively
	def addGeometryCol (tableName: String) {
		// open connection
		val sqlConnect = java.sql.DriverManager.getConnection(Constants.DB_URL, Constants.DB_USER, 
																Constants.DB_PASSWORD)
		// add the column
		val addColumn = sqlConnect.prepareStatement("alter table " + tableName +
													 " add column geom geometry(Point, 4326)")
		addColumn.executeUpdate()
		addColumn.close()

		// now compute the geometry
		val computeGeometry = sqlConnect.prepareStatement("update " + tableName + 
										" set geom = ST_SetSRID(ST_point(\"Lon\",\"Lat\"), 4326)")
		computeGeometry.executeUpdate()
		computeGeometry.close()

		// finally, drop the index if it already exists and recreate
		val dropIndex = sqlConnect.prepareStatement("drop index if exists " + tableName + "_index")
		dropIndex.executeUpdate()
		dropIndex.close()

		val addIndex = sqlConnect.prepareStatement("create index " + tableName + "_index on " +
													tableName + "(geom)")
		addIndex.executeUpdate()
		addIndex.close()

		sqlConnect.close()
	} // end addGeometryCol

	/**
	 * This function reads in all the data from the NOAA directory and returns a 
	 * dataframe with the schema:
	 * ID, lat, lon, state, stat, date, 2019-data, 2018-data, 3-year, 3y_count, 5-year,
	 * 5y_count, 10-year, 10y_count, 20-year, 20y_count, all, all_count
	 */
	def getWeatherStats(spark: SparkSession) = {
	 	// add ability to use dataframes
	 	import spark.implicits._

	 	// get the 2019 data - most recent when created
	 	var years = getWeatherForYear(spark, 2019)

	 	// start weatherStats with renamed value column
	 	var weatherStats = years.withColumnRenamed("Value", "2019_value").drop("Year")

	 	// read in the data, year by year, storing certain information as needed
	 	for (year <- 2018 to 1990 by -1) {
	 		var current = getWeatherForYear(spark, year)
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
	 	//weatherStats = getAverages("all", weatherStats, years)

		weatherStats
	} // end getWeatherStats

	// Given the appropriate spark context, spark session, and year, returns a dataframe
	// containing all data from that year
	def getWeatherForYear(spark: SparkSession, yyyy: Int) = {
		import spark.implicits._

		// create schema for reading in year data
	 	val yearSchema = StructType(
	 		List(
	 			StructField("Station_ID", StringType, false),
	 			StructField("Date", StringType, false),
	 			StructField("Stat", StringType, false),
	 			StructField("Value", IntegerType, false),
	 			StructField("M", StringType, true),
	 			StructField("Q", StringType, true),
	 			StructField("S", StringType, true),
	 			StructField("OBS-TIME", StringType, true)
	 		)
	 	)

		// return in the relevant data
		var current = spark.read.format("csv").option("header","false")
						.schema(yearSchema)
						.load(Constants.NOAA_DATA_DIR + yyyy.toString + ".csv")


		// specifying the desired measurements
		val desiredElements = Seq("PRCP","SNOW","SNWD","TAVG","TMAX","TMIN","TOBS",
								  "WT01","WT02","WT03","WT04","WT05","WT06","WT07",
								  "WT08","WT09","WT10","WT11","WT12","WT13","WT14",
								  "WT15","WT16","WT17","WT18","WT19","WT20","WT21","WT22")

		// convert date column into three columns: year, month, day
		current.filter($"Station_ID".startsWith("US"))
			   .filter(($"Stat").isin(desiredElements: _*))
			   .withColumn("Year", substring($"Date",0,4).cast("int"))
			   .withColumn("Month", substring($"Date",5,2).cast("int"))
			   .withColumn("Day", substring($"Date",7,2).cast("int"))
			   .select("Station_ID","Year","Month","Day","Stat","Value")
	}

	/**
	 * This function counts and averages the data stored in years by station, date,
	 * and measurement statistic then joins it with the weatherStats dataframe. The
	 * string is used to set the newly added column names.
	 */
	def getAverages(name: String, weatherStats: DataFrame, years: DataFrame) = {
	 	var stats = years.groupBy("Station_ID","Month","Day","Stat")
	  					 .agg(mean("Value"),count("Value"))
	  					 .withColumnRenamed("avg(Value)", name + "_year_avg")
	  					 .withColumnRenamed("count(Value)", name + "_year_count")
	  	weatherStats.join(stats, Seq("Station_ID", "Month", "Day", "Stat"), joinType = "full")
	} // end getAverages

	// This function creates a dataframe where each hike is associated to all weather stations
	// within 50km of that hike's GPS coordinate.
	def mapHikesToStations (hikes: DataFrame) = {

		val spark = hikes.sparkSession
		import spark.implicits._
		val sc = spark.sparkContext

		// to get the stations close to each hike, we collect the hike dataframe
		// on the master node
		val hikesOnMaster = hikes.collect()

		// add a column that contains a list of the stations close to the hike
		val hikesWithStations = hikesOnMaster.map {
			case Row(id: String, lat: Double, lon: Double, name: String) => {
				(id, lat, lon, name, getCloseStations(lat, lon, spark))
			}
		}

		// now we parallelize again and give the columns descriptive names
		var hikesDF = sc.parallelize(hikesWithStations).toDF

		hikesDF = hikesDF.withColumnRenamed("_1","Hike_ID").withColumnRenamed("_2","Lat")
						 .withColumnRenamed("_3","Lon").withColumnRenamed("_4","Name")
						 .withColumnRenamed("_5","Stations")

		hikesDF.withColumn("Station_ID", explode($"Stations")).drop("Stations")

	} // end mapHikesToStations

	// Given a latitude/longitude point, queries the stations PostGIS table and returns
	// the ID of all stations within 50km
	def getCloseStations (lat: Double, lon: Double, spark: SparkSession) = {
		import spark.implicits._

		val closeStations = "(select \"Station_ID\" from stations where st_dwithin(geom, st_makepoint(" + 
							lon + ", " + lat + ")::geography, 50000)) as foo"

		val stations = spark.read.format("jdbc").option("url", Constants.DB_URL)
				.option("dbtable", closeStations).option("user", Constants.DB_USER)
				.option("password", Constants.DB_PASSWORD)
				.option("driver", "org.postgresql.Driver").load()

		stations.select("Station_ID").map(r => r.getString(0)).collect.toList
	} // end getCloseStations


	// Given the dataframe that contains the association between hikes and station
	// readings, we now compute the desired averages for each hike and write the
	// results to the database
	def storeWeatherAverages (hikeData: DataFrame) {
		import hikeData.sparkSession.implicits._

		// the NOAA data has been simplified with average and count columns, which
		// need to be multiplied for correct future averages
		val expandAverages = hikeData.withColumn("3_year_tot", $"3_year_avg" * $"3_year_count")
									 .withColumn("5_year_tot", $"5_year_avg" * $"5_year_count")
									 .withColumn("10_year_tot", $"10_year_avg" * $"10_year_count")
									 .withColumn("20_year_tot", $"20_year_avg" * $"20_year_count")
									 .withColumn("30_year_tot", $"30_year_avg" * $"30_year_count")

		// now to sum the totals and counts, grouped by hike, date, and weather statistic
		val sumColumns = expandAverages.groupBy("Hike_ID","Month", "Day", "Stat")
						.agg(mean("2019_value").alias("2019_value"), mean("2018_value").alias("2018_value"),
							sum("3_year_tot").alias("3_year_num"), sum("3_year_count").alias("3_year_denom"),
							sum("5_year_tot").alias("5_year_num"), sum("5_year_count").alias("5_year_denom"),
							sum("10_year_tot").alias("10_year_num"), sum("10_year_count").alias("10_year_denom"),
							sum("20_year_tot").alias("20_year_num"), sum("20_year_count").alias("20_year_denom"),
							sum("30_year_tot").alias("30_year_num"), sum("30_year_count").alias("30_year_denom"))

		// finally, we compute the appropriate averages and drop the no longer relevant columns
		val hikeAverages = sumColumns.withColumn("3_year_avg", $"3_year_num" / $"3_year_denom")
									 .withColumn("5_year_avg", $"5_year_num" / $"5_year_denom")
									 .withColumn("10_year_avg", $"10_year_num" / $"10_year_denom")
									 .withColumn("20_year_avg", $"20_year_num" / $"20_year_denom")
									 .withColumn("30_year_avg", $"30_year_num" / $"30_year_denom")
									 .drop("3_year_num", "3_year_denom", "5_year_num", "5_year_denom", 
									 	"10_year_num", "10_year_denom", "20_year_num", "20_year_denom",
									 	"30_year_num", "30_year_denom")

		// with the processing done, we write the data to our database
		hikeAverages.write.format("jdbc").option("url", Constants.DB_URL)
			 .option("dbtable","hikeWeather").option("user", Constants.DB_USER)
			 .option("password", Constants.DB_PASSWORD)
			 .option("driver", "org.postgresql.Driver").mode("overwrite").save()
	} // end storeWeatherAverages


} // end Batch object