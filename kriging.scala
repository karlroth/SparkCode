//Run:
// $ spark-shell --packages=org.locationtech.geotrellis:geotrellis-vector_2.11:1.0.0
// scala> :load kriging.scala

/*

TO DO:
-Filter out data on Quad
-Determine grid size on Quad
-Should the data be normalized? (ie Northing, Easting...)

*/
import geotrellis.vector.interpolation._ 
import geotrellis.vector._ 


//Set file
val file =  "s3://myeonghun/radiation_data_201601_20160510/may.csv"

val data = spark.read.option("inferSchema", "true").csv(file)
val wanted = data.select("_c1","_c2","_c3").as[(Double, Double, Double)].filter($"_c1" > 40.109630).filter($"_c1" < 40.114630).filter($"_c2" > -88.22900).filter($"_c2" < -88.22400).filter($"_c3" > 0)



//Input data points 
val rdd1 = wanted.rdd.map(c => {
  val lat = c._1 - 40.109630
  val lng = c._2 + 88.22900
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


//Array of Points to krige
//10x10 (50 square meters)
/*
val lat = (40.109880 to 40.114380 by 0.0005).toList
val lon = (-88.228750 to -88.224250 by 0.0005).toList
*/

//5x5 (100 square meters)
val lat = (40.109880 to 40.114380 by 0.001).toList
val lon = (-88.228750 to -88.224250 by 0.001).toList


val grid = lat.zip(lon).toDS()

val rdd2 = grid.rdd.map(c => {
  val result = Point(c._1,c._2)
  (result)
})

val locations = rdd2.collect()

val attrFunc: (Double, Double) => Array[Double] = {
  (x, y) => Array(x, y, x * x, x * y, y * y)
}




/*
val nonLinearSV: Semivariogram =
    NonLinearSemivariogram(points, 1, 30, Spherical)
*/
/*
val krigingVal: Array[(Double, Double)] =
  new UniversalKriging(points, attrFunc, 50, Spherical).predict(locations)

*/

