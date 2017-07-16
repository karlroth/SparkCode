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

val df_clean = df3.filter($"Sigma" > 0).filter($"Latitude" > 40.109028).filter($"Latitude" < 40.116430).filter($"Longitude" < -88.222401).filter($"Longitude" > -88.230338)

//Convert from unix to dd-MM-yyyy, and round the latitude and longitude 
val df_formated = df_clean.withColumn("day", from_unixtime($"Time", "dd-MM-yyyy ")).withColumn("localTime", from_unixtime($"Time", "HH:mm:ss")).withColumn("roundedLat", round($"Latitude", 5)).withColumn("roundedLon", round($"Longitude", 5))
df_formated.registerTempTable("d3data")

//Map reduce function to determine detector with most records

//First select only the detector id column, then add a new 
//column with count = 1 for each record.
//This maps each detector id to the value 1.
val detectors = sqlContext.sql("SELECT ID FROM d3data")
val map = detectors.withColumn("count",lit(1))
map.registerTempTable("map")

//Next reduce the dataset by grouping by id and summing
//together the counts. List in decsending order to retreive
//the detector with the most records.
val reduce = sqlContext.sql("SELECT ID,COUNT(*) AS count FROM map GROUP BY ID ORDER BY count DESC")
//reduce.take(10)

//RESULT: D3-SGM100224,3931794
//
//
//Get some basic statistics on our detector
val step1 = sqlContext.sql("SELECT roundedLat,roundedLon,Sigma,day,localTime,Time FROM d3data WHERE id = 'D3-SGM100224'")
step1.registerTempTable("detector")
//step1.describe().show()

//Retrieve data only from the North Quad
val step2 = sqlContext.sql("SELECT * FROM detector WHERE roundedLat > 40.111471 AND roundedLat < 40.112206 AND roundedLon > -88.227652 AND roundedLon < -88.226517")
step2.registerTempTable("Quad")
//step2.describe().show()

//Filter out gamma-ray counts: 91 based on interquartile ranges
//We don't need to worry about negative values. Filtered out earlier.
val step3 = sqlContext.sql("SELECT * FROM Quad WHERE Sigma < 91")
step3.registerTempTable("Filtered")

//ORDER
val step4 = sqlContext.sql("SELECT * FROM Filtered ORDER BY Time")

//generate new records based on latitude and longitude key.
val step5 = step4.withColumn("lonKey", ($"roundedLon" + 88.227652)/0.0001).withColumn("latKey", ($"roundedLat" - 40.111471)/0.001) 
step5.registerTempTable("Keys")

val step5_avg = sqlContext.sql("SELECT latKey,lonKey,AVG(Sigma) AS avgSigma FROM Keys GROUP BY (latKey,lonKey)")

//To save the file, it is easiest to convert these to an RDD and save as text file:
step5_avg.cache()
val result = step5_avg.rdd

step5_avg.saveAsTextFile("oneDetector2.txt")
// This will save these files into hdfs.  To find and get them:
// hadoop fs -ls
// hadoop fs -get det.txt
// etc.