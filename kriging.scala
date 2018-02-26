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
val file =  "s3://myeonghun/radiation_data_201601_20160510/radiation.csv"

//Read file
val data = spark.read.option("inferSchema", "true").csv(file)

/*Filter on latitude, longitude, and time. Exclude negative counts per minute (cpm)
* _c1 -> latitude
* _c2 -> longitude
* _c3 -> Sigma (cpm)
* _c5 -> Time (unix miliseconds)
*/
val wanted = data.select("_c1","_c2","_c3","_c5").as[(Double, Double, Double, Int)].filter($"_c1" > 40.10902).filter($"_c1" < 40.116430).filter($"_c2" > -88.23033).filter($"_c2" < -88.223750).filter($"_c3" > 0).filter($"_c3" < 91).filter($"_c5" >  1457676000).filter($"_c5" < 1457762400)



//Input data points 
val rdd1 = wanted.rdd.map(c => {
  val lat = c._1 - 40.109028
  val lng = c._2 + 88.23033
  val point = Point(lat, lng)
  val feature = Feature(point, c._3)
  (feature)
})


/*re
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
/*
// lower left corner (40.109028,-88.230330)
// top right corner  (40.116430, -88.223750)

val latSplit = List(40.109028,40.111495,40.113962,40.116430)
val lonSplit = List(-88.230330,-88.228137,-88.225944,-88.223751)

for(x <- 1 to 3) {
  for(y <- 1 to 3) {
    val lat = (latSplit(x-1) to latSplit(x) by 0.0001).toList
    val lon = (lonSplit(y-1) to lonSplit(y) by 0.0001).toList


  }
}

*/

/*
//66x66 (10 square meters)
val lat = (40.109028 to 40.116430 by 0.0001).toList
val lon = (-88.23033 to -88.223750 by 0.0001).toList
*/

//22x22 (10 square meters)
val lat = (40.109028 to 40.111495 by 0.001).toList
val lon = (-88.23033 to -88.228137 by 0.001).toList

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
val nonLinearSV: Semivariogram = NonLinearSemivariogram(points, 1, 30, Spherical)
*/

val krigingVal: Array[(Double, Double)] =
  new UniversalKriging(points, attrFunc, 5000, Spherical).predict(locations)


