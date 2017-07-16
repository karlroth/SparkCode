import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.joda.time.{DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.DataFrameStatFunctions

//Retrieve the Data-set
val file = "s3://myeonghun/radiation_data_201601_20160510/radiation.csv"
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val df = sqlContext.read.format("com.databricks.spark.csv").option("header","false").option("inferSchema","true").load(file)
//df.take(10)

//Clean the data set and add proper labels
//val df2 = df.select("_c0", "_c1","_c2","_c3","_c5")
val df2 = df.select("_c1","_c2","_c3")

//val newNames = Seq("id", "lat", "lon", "Grosscount","time")
val newNames = Seq("lat","lon","Grosscount")

val df3 = df2.toDF(newNames: _*)

val df_clean = df3.filter($"Grosscount" > 0).filter($"lat" > 40.109028).filter($"lat" < 40.116430).filter($"lon" < -88.222401).filter($"lon" > -88.230338)

//Convert from unix to dd-MM-yyyy, and round the latitude and longitude 
val df_formated = df_clean.withColumn("roundedLat", round($"lat", 5)).withColumn("roundedLon", round($"lon", 5))

df_formated.registerTempTable("d3data")

val avg = sqlContext.sql("SELECT AVG(Grosscount) FROM d3data")
//val dev = df_formated.agg(stddev_pop($"Grosscount"))

val mean = avg.first().getDouble(0)

//val stdev = dev.first().getDouble(0)
val stdev = math.sqrt(mean)

val threeSigma = 3*stdev
val trigger = threeSigma + mean
val alarm = sqlContext.sql("SELECT * FROM d3data WHERE Grosscount >"+ trigger)

alarm.cache()
val alarmRDD = alarm.rdd 

alarmRDD.saveAsTextFile("exp_alarms.txt")