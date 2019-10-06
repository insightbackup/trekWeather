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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

import Constants._
import Reader._
import NoaaProcess._

object Batch {

	def main(args: Array[String]) {

		// Set up spark context
		val conf = new SparkConf().setAppName("Batch")
		val sc = new SparkContext(conf)
		val spark = SparkSession.builder.appName("Batch").
							config("spark.master", "local").getOrCreate()
		import spark.implicits._

		// get weather station information and write it to database
	  	Reader.createStationsTable(sc, spark)

	  	// get hike information and write it to database
	  	val hikes = Reader.createHikesTable(spark)

		// load averaged US weather data into dataframe
		val noaaData = NoaaProcess.getWeatherStats(spark)

		// associate each hike to the stations close to it
		val hikesDF = mapHikesToStations(hikes)

		// join the weather stats to the hikes
		val hikesWithWeather = hikesDF.join(noaaData, "Station_ID")

		// repartition the dataframe to rebalance the size after the column 
		// explosion and join. Then compute the weather averages for each hike
		// and write result to database
		storeWeatherAverages(hikesWithWeather.repartition($"Hike_ID"))

	} // end main


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
		// this step requires querying the database once per row
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