/** Reader
 *
 * This object contains the methods that pull the relevant data from S3 and 
 * perform light processing to store it in an appropriate dataframe.
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import Constants._

object Reader {


	// Given the appropriate spark context and spark session, returns a dataframe
	// containing the ID and location of all US weather stations
	def getStations(sc: SparkContext, spark: SparkSession) = {
		import spark.implicits._

		val stations = sc.textFile(Constants.NOAA_STATIONS)

		// split into pieces: ID, lat, lon, state
		val splitLines = stations.map(line => (line.substring(0,11), 
		      line.substring(12,20), line.substring(21,30), line.substring(38,40)))

		// filter by stations in the US
		val usStations = splitLines.filter(x => x._1.substring(0,2) == "US")

		usStations.toDF("ID", "Lat", "Lon", "State")
				.withColumn("Lat", $"Lat".cast("double"))
				.withColumn("Lon", $"Lon".cast("double"))
	}


	// Given the appropriate spark context, spark session, and year, returns a dataframe
	// containing all data from that year
	def getWeatherForYear(sc: SparkContext, spark: SparkSession, yyyy: Int) = {
		import spark.implicits._

		// create schema for reading in year data
	 	val yearSchema = StructType(
	 		List(
	 			StructField("ID", StringType, false),
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
		current.filter($"ID".startsWith("US"))
			   .filter(!($"Value").isin(desiredElements: _*))
			   .withColumn("Year", substring($"Date",0,4).cast("int"))
			   .withColumn("Month", substring($"Date",5,2).cast("int"))
			   .withColumn("Day", substring($"Date",7,2).cast("int"))
			   .select("ID","Year","Month","Day","Stat","Value")


	}

}