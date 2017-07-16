//Include Alma, Church, NRL, and Talbot
//Make sure there are some days with sources
//Jan 1 - May 1 2016
//Can they find anonalous sources?
//
//We will know when source is present

import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.joda.time.{DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.DataFrameStatFunctions

import org.apache.spark.SparkContext._

val file = "s3://myeonghun/radiation_data_201601_20160510/radiation.csv"
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val df = sqlContext.read.format("com.databricks.spark.csv").option("header","false").option("inferSchema","true").load(file)

//Clean the data set and add proper labels
val df2 = df.select("_c0", "_c1","_c2","_c3","_c5")

val newNames = Seq("ID", "Latitude", "Longitude", "Sigma","Time")
val df3 = df2.toDF(newNames: _*)

//Filter out:
//Latitude 40.109630 to 40.112900
//Longitude -88.229000 to -88.223800
//Filter from Jan 1 - May 1 2016
val df_clean = df3.filter($"Sigma" > 0).filter($"Latitude" > 40.109630).filter($"Latitude" < 40.112900).filter($"Longitude" > -88.229000).filter($"Longitude" < -88.223800).filter($"Time" > 1451606400).filter($"Time" < 1462060800)

df_clean.cache()

val dataRDD = df_clean.rdd

dataRDD.saveAsText("data")
