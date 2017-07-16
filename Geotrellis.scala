// In AWS EMR isntall git:
$ sudo yum install git-core

// Then retrieve repository:
$ git clone https://github.com/geotrellis/geotrellis-sbt-template.git

// Add the Library Dependencies to build.sbt:
$ libraryDependencies ++= Seq(
    "org.locationtech.geotrellis" %% "geotrellis-spark"  % "1.0.0",
    "org.locationtech.geotrellis" %% "geotrellis-s3"     % "1.0.0", // now we can use Amazon S3!
    "org.apache.spark"            %% "spark-core"        % "2.1.0" % "provided",
    "org.scalatest"               %% "scalatest"         % "3.0.0" % "test",
    "" %% "geotrellis-vector" % "1.0.0"
)
// In the geotrellis-template folder run:
$ ./sbt

// To start spark run in sbt:
> console

//You should see the following:
scala> 


//ALTERNATIVELY
$ spark-shell --packages=org.locationtech.geotrellis:geotrellis-spark_2.11:1.0.0,org.locationtech.geotrellis:geotrellis-s3_2.11:1.0.0,org.locationtech.geotrellis:geotrellis-vector_2.11:1.0.0

//Vectors only
$ spark-shell --packages=org.locationtech.geotrellis:geotrellis-vector_2.11:1.0.0
//Enter all comands in the above terminal

//Define functions we will use later, to convert strings to doulbes
//and doubles to points respectively.
case class Point(x: Double, y: Double) extends geotrellis.vector.Point(x,y)

//case class Point(x: Double, y: Double) extends com.vividsolutions.jts.geom.Point(x,y)
case class Feature(p: Point, d: Double)

val toDouble = udf[Double, String](_.toDouble)
val toPoint = udf((x: Double, y: Double) => Point(x, y)))
val toPointFeature = udf((p: Point, d: Double) => Feature(p,d))

//Import desired file into PointFeature Array
//Where _c1 = Latitude 
//		_c2 = Longitude
//		_c3 = Sigma
/*
val data = spark.read.csv(file)
val wanted = data.select("_c1","_c2","_c3")
				 .withColumn("lat", toDouble(wanted("_c1")))
				 .withColumn("long", toDouble(wanted("_c2")))
				 .withColumn("sigma", toDouble(wanted("_c3")))

val point = wanted.withColumn("Point", toPoint(watned("lat"),wanted("long")))
val features = points.withColumn("Feature", toPointFeature(point("Point"), point("sigma")))

val points = features.select("Feature")
val location = point.select("Point")
*/



/*

TO DO:
-Filter out data on Quad
-Determine grid size on Quad
-Should the data be normalized? (ie Northing, Easting...)

*/
import geotrellis.vector.interpolation._ //UniversalKriging
import geotrellis.vector._ 


//Set file
val file =  "s3://myeonghun/radiation_data_201601_20160510/radiation.csv"

val data = spark.read.option("inferSchema", "true").csv(file)
val wanted = data.select("_c1","_c2","_c3").as[(Double, Double, Double)]

//Input data points 
val rdd1 = wanted.rdd.map(c => {
  val point = Point(c._1, c._2)
  val feature = Feature(point, c._3)
  (feature)
})

val points =  rdd1.collect()

//Array of Points to krige
val lat = (40.109880 to 40.114380 by 0.0005).toList
val lon = (-88.228750 to -88.224250 by 0.0005).toList
val grid = lat.zip(lon).toDS()

val rdd2 = grid.rdd.map(c => {
  val result = Point(c._1,c._2)
  (result)
})

val locations = rdd2.collect()

val attrFunc: (Double, Double) => Array[Double] = {
  (x, y) => Array(x, y, x * x, x * y, y * y)
}

val krigingVal: Array[(Double, Double)] =
  new UniversalKriging(points, attrFunc, 50, Spherical).predict(locations)


/*
val data = spark.read.option("inferSchema", "true").csv(file)
val wanted = data.select("_c1","_c2","_c3").as[(Double, Double, Double)]
val rdd = wanted.rdd.map(c ⇒ {
  val point = Point(c._1, c._2)
  val feature = Feature(point, c._3)
  (point, feature)
})

val (pointsOnly, featuresOnly) =  rdd.collect().unzip

val krigingVal: Array[(Double, Double)] =
  new UniversalKriging(featuresOnly, attrFunc, 50, Spherical) .predict(pointsOnly)
  */

val toPoint = udf((x: Double, y: Double) => Point(x, y))










