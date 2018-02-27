/** 
 * @author Karl Roth
 *
 * The semivariogram is the most computationally intensive step of the Kriging process. 
 * In order to parallelize this we must determine how large of a dataset this function can
 * perform before crashing. 
 * The dataset to be sampled will be from the single day March 11, 2016. 
 * This day was chosen because it has the most data to work
 * with of any given day. 337455 data points, with clear weather. 
 *
 *
 * To load third party packages run:
 * $ spark-shell --packages=org.locationtech.geotrellis:geotrellis-vector_2.11:1.0.0
 *
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


import geotrellis.vector.interpolation._
import geotrellis.vector._


val file = "s3://myeonghun/radiation_data_201601_20160510/radiation.csv"
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val rawDF = sqlContext.read.format("com.databricks.spark.csv").option("header","false").option("inferSchema","true").load(file)

/* Clean the data set and add proper labels */
val selectDF = rawDF.select("_c0", "_c1","_c2","_c3","_c5")

val newNames = Seq("ID", "Latitude", "Longitude", "Sigma","Time")
val labelDF = selectDF.toDF(newNames: _*)

/**
 * Retrieve data only from UIUC Engineering Campus
 *  for the day March 11, 2016 with non-negative counts 
 */
val cleanDS = labelDF.filter($"Sigma" > 0).filter($"Latitude" > 40.109028).filter($"Latitude" < 40.116430).filter($"Longitude" > -88.230338).filter($"Longitude" < -88.223750).filter($"Time" > 1457676000).filter($"Time" < 1457762400)

val featuresDS = cleanDF.select("Latitude","Longitude","Sigma").as[(Double,Double,Double)]

/**
 * Now this filtered data will be sampled using the SparkSampling class.
 * This sampling function takes three variables:
 *	
 *   1. is the sampling done with replacement
 *	 2. the sample size as a fraction
 *	 3. optional, random seed.
 */

/* Sample the raw data */
val sampleDataDS = featuresDF.sample(false, 0.01, 1234);

/**
 * val sampleDataSize = sampleData.count();
 * val rawDataSize = rawData.count();
 * System.out.println(rawDataSize + " and after the sampling: " + sampleDataSize);
 */ 


/** 
 * It's important to note that after sampling the DataSet is converted from org.apache.spark.sql.Dataset[(Double, Double, Double, Int)] 
 * to org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] 
 *
 * This means that when mapping we will have to use value.getDouble(1) instead of value._1 to access the elements of the RDD[Row]
 */

/* Replace null values */


//Input data points
val featuresRDD = sampleDataDS.rdd.map(row => {
      val lat = row._1 - 40.109028
      val lng = row._2 + 88.230338
      val point = Point(lat, lng)
      val feature = Feature(point, row._3)
      (feature)
     })

val points =  featuresRDD.collect()

val nonLinearSV: Semivariogram = NonLinearSemivariogram(points, 30, 30, Spherical)

