semivariogram.scala

import geotrellis.vector.interpolation._ 
import geotrellis.vector._ 


/* Set file */
val file =  "s3://myeonghun/radiation_data_201601_20160510/radiation.csv"

/* Read file */
val data = spark.read.option("inferSchema", "true").csv(file)

/**
 * Filter on latitude, longitude, and time. Exclude negative counts per minute (cpm)
 *   _c1 -> latitude
 *   _c2 -> longitude
 *   _c3 -> Sigma (cpm)
 *  _c5 -> Time (unix miliseconds)
 */
val wanted = data.select("_c1","_c2","_c3","_c5").as[(Double, Double, Double, Int)].filter($"_c1" > 40.10902).filter($"_c1" < 40.111495).filter($"_c2" > -88.23033).filter($"_c2" < -88.228137).filter($"_c3" > 0).filter($"_c3" < 91).filter($"_c5" >  1457676000).filter($"_c5" < 1457762400)



//Input data points 
val rdd1 = wanted.rdd.map(c => {
  val lat = c._1 - 40.109028
  val lng = c._2 + 88.23033
  val point = Point(lat, lng)
  val feature = Feature(point, c._3)
  (feature)
})

/*
val rdd1 = wanted.rdd.map(c => {

  val point = Point(c._1, c._2)
  val feature = Feature(point, c._3)
  (feature)
})
*/

val points =  rdd1.collect()

val nonLinearSV: Semivariogram = NonLinearSemivariogram(points, 30, 30, Spherical)