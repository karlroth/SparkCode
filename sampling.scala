/**
 * @author Karl Roth
 * @since Feb 2, 2018
 * 
 * This purpose of this program is sample a dataset for the purposes of testing and training algotrithms. 
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

/** 
 * The dataset will be filtered based off of previous experimental conditions. 
 * Taking data from March 11, 2016 (the most populous day of our data set)
 * from the engineering campus (Bardeen Quad, North Quad)
 * including the elevated radiation areas: Nuclear Radiation Lab, the Alma Mater, 
 * and the Wesley United Methodist Church
 */

val file = "s3://myeonghun/radiation_data_201601_20160510/radiation.csv"
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val df = sqlContext.read.format("com.databricks.spark.csv").option("header","false").option("inferSchema","true").load(file)

/* Clean the data set and add proper labels */
val df2 = df.select("_c0", "_c1","_c2","_c3","_c5")

val newNames = Seq("ID", "Latitude", "Longitude", "Sigma","Time")
val df3 = df2.toDF(newNames: _*)

/** 
 * Retrieve data only from UIUC Engineering Campus
 * for the day March 11, 2016 with non-negative counts 
 */  
val df_clean = df3.filter($"Sigma" > 0).filter($"Latitude" > 40.109028).filter($"Latitude" < 40.116430).filter($"Longitude" > -88.230338).filter($"Longitude" < -88.223750).filter($"Time" > 1457676000).filter($"Time" < 1457762400)

/* Convert from unix to dd-MM-yyyy, and round the latitude and longitude */
val df_formated = df_clean.withColumn("day", from_unixtime($"Time", "dd-MM-yyyy ")).withColumn("localTime", from_unixtime($"Time", "HH:mm:ss")).withColumn("roundedLat", round($"Latitude", 5)).withColumn("roundedLon", round($"Longitude", 5))


/**
 * Now this filtered data will be sampled using the SparkSampling class.
 * This sampling function takes three variables:
 *	
 * 	 1. is the sampling done with replacement
 *	 2. the sample size as a fraction
 *	 3. optional, random seed.
 */


val rawData = df_formated

/* Sample the raw data */ 
val sampledData = rawData.sample(false, 0.1, 1234);
   
val sampleDataSize = sampledData.count();
val rawDataSize = rawData.count();
System.out.println(rawDataSize + " and after the sampling: " + sampleDataSize);

/** 
 * It's important to note that after sampling the DataSet is converted from org.apache.spark.sql.Dataset[(Double, Double, Double, Int)] 
 * to org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] 
 *
 * gThis means that when mapping we will have to use value.getDouble(1) instead of value._1 to access the elements of the RDD[Row]
 */










