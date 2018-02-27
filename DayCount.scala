/** 
 * @author Karl Roth
 *
 * This script was used to find the Days with the most data points. 
 * Weather Underground was used to determine the weather conditions on that day. 
 *
 * Run:
 *  spark-shell --packages com.databricks:spark-csv_2.11:1.5.0
 */

import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.joda.time.{DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.DataFrameStatFunctions
import org.apache.spark.sql.types.IntegerType

import org.apache.spark.SparkContext._



val file = "s3://myeonghun/radiation_data_201601_20160510/radiation.csv"
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val rawDF = sqlContext.read.format("com.databricks.spark.csv").option("header","false").option("inferSchema","true").load(file)

/* Clean the data set and add proper labels */
val selectDF = rawDF.select("_c0", "_c1","_c2","_c3","_c5")

val newNames = Seq("ID", "Latitude", "Longitude", "Sigma","Time")
val newNamesDF = selectDF.toDF(newNames: _*)

val cleanDS = newNamesDF.filter($"Sigma" > 0).filter($"Latitude" > 40.109028).filter($"Latitude" < 40.116430).filter($"Longitude" > -88.230338).filter($"Longitude" < -88.223750)

/* Convert from unix to dd-MM-yyyy, and round the latitude and longitude */
val formatedDF = cleanDS.withColumn("day", from_unixtime($"Time", "dd-MM-yyyy ")).withColumn("localTime", from_unixtime($"Time", "HH:mm:ss")).withColumn("roundedLat", round($"Latitude", 5)).withColumn("roundedLon", round($"Longitude", 5))
df_formated.registerTempTable("d3data")

/**
 * Map reduce function to determine detector with most records
 *
 * First select only the day column, then add a new 
 * column with count = 1 for each record.
 * This maps each day to the value 1.
 */
val detectorsDF = sqlContext.sql("SELECT day FROM d3data")
val mapDF = detectorsDF.withColumn("count",lit(1))
mapDF.registerTempTable("map")

/**
 * Next reduce the dataset by grouping by day and summing
 * together the counts. List in decsending order to retreive
 * the detector with the most records.
 */
val reduceDF = sqlContext.sql("SELECT day,COUNT(*) AS count FROM map GROUP BY day ORDER BY count DESC")
reduceDF.take(10)

/** 
 * RESULTS:
 * 
 *   dd-mm-yyyy, counts weather
 *
 *   11-03-2016, 337455 Clear
 *   09-03-2016, 317598 Overcast
 *   08-02-2016, 306571 Snow
 *   17-02-2016, 302635 Overcast
 *   10-03-2016, 300460 Rain
 *   07-02-2016, 296509 Clear
 *   08-03-2016, 293733 Overcast
 *   16-02-2016, 266554 Overcast
 *   14-03-2016, 262518 Overcast/Fog
 *   28-03-2016, 257419 Overcast
 *
 * Weather data collected from wunderground.com
 *
 */

val reduceDF = sqlContext.sql("SELECT day,COUNT(*) AS count FROM map GROUP BY day ORDER BY count ASC")
reduceDF.take(10)

/** 
 * RESULTS:
 *   dd-mm-yyyy, counts weather
 *
 *   20-03-2016, 338    Clear 
 *   28-02-2016, 11119  Overcast
 *   11-05-2016, 14389  Snow
 *   23-04-2016, 14910  Overcast
 *   14-01-2016, 23904  Rain
 *   26-01-2016, 25994  Clear
 *   22-03-2016, 28089  Overcast
 *   22-04-2016, 29226  Overcast
 *   21-04-2016, 29521  Overcast/Fog
 *   13-01-2016, 30915  Overcast
 *
 *   20-03-2016 => ( 1458432000 to 1458518399 )
 *
 * Weather data collected from wunderground.com
 *
 */















