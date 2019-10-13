/** Reader object for Trek Weather project
 *
 * This object handles loading data into Spark from S3 and writing the stations
 * list and hikes list to PostGIS.
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
import java.sql.DriverManager
import java.sql.Connection

import Constants._

object Reader {
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

		// First, filter for ways and relations (instead of nodes) and recode the 16 digit Hike_ID
		// in hex for use in the database. Then filter for hikes that have names and then remove '
		// and " from hike names for use in JSON with Flask
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
													tableName + " using gist(geom)")
		addIndex.executeUpdate()
		addIndex.close()

		sqlConnect.close()
	} // end addGeometryCol



	// Given the appropriate spark session and year, returns a dataframe
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


		// Specifying the desired measurements - not all are used in current application
		// but are available in the database.
		// For the meaning of these, see the NOAA data readme available at 
		// https://docs.opendata.aws/noaa-ghcn-pds/readme.html
		val desiredElements = Seq("PRCP","SNOW","SNWD","TAVG","TMAX","TMIN","TOBS",
								  "WT01","WT02","WT03","WT04","WT05","WT06","WT07",
								  "WT08","WT09","WT10","WT11","WT12","WT13","WT14",
								  "WT15","WT16","WT17","WT18","WT19","WT20","WT21","WT22")

		// Remove undesired stats and any measurement that has a quality flag (<.1% of 
		// total measurements of desired stats per year)
		// convert date column into three columns: year, month, day
		current.filter($"Station_ID".startsWith("US"))
			   .filter(($"Stat").isin(desiredElements: _*))
			   .filter($"Q".isNull)
			   .withColumn("Year", substring($"Date",0,4).cast("int"))
			   .withColumn("Month", substring($"Date",5,2).cast("int"))
			   .withColumn("Day", substring($"Date",7,2).cast("int"))
			   .select("Station_ID","Year","Month","Day","Stat","Value")
	} // end getWeatherForYear


} // end Reader