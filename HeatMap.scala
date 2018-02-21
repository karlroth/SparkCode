import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.joda.time.{DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.DataFrameStatFunctions

import com.vividsolutions.jts.geom.Envelope;

import java.awt.Color;

import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;

import org.datasyslab.geospark.spatialOperator.JoinQuery;

import org.datasyslab.babylon.core.OverlayOperator;
import org.datasyslab.babylon.extension.imageGenerator.NativeJavaImageGenerator;
import org.datasyslab.babylon.extension.visualizationEffect.ChoroplethMap;
import org.datasyslab.babylon.extension.visualizationEffect.HeatMap;
import org.datasyslab.babylon.extension.visualizationEffect.ScatterPlot;

val USMainLandBoundary = new Envelope(-126.790180,-64.630926,24.863836,50.000);

// Scatter plot
val counts = new PointRDD(sc, "s3://myeonghun/radiation_data_201601_20160510/may.csv", 2,FileDataSplitter.CSV);
val scatterPlot = new ScatterPlot(1000,600,USMainLandBoundary,false);
scatterPlot.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
scatterPlot.Visualize(sc, counts);

val imageGenerator = new NativeJavaImageGenerator();
imageGenerator.SaveAsFile(scatterPlot.pixelImage, "s3://karl-roth/testScatter", ImageType.PNG);

//Heat map
val zipcodearea = new RectangleRDD(sc, "s3://myeonghun/radiation_data_201601_20160510/may.csv", 0, FileDataSplitter.CSV, false, 4);
val heatMap = new HeatMap(800,500,USMainLandBoundary,false,5);
heatMap.Visualize(sc, zipcodearea, "./babylon/currentEffect.png");
heatMap.JoinImages(heatMap.pixelImage, scatterPlot.pixelImage, "./babylon/currentEffect.png");

val imageGenerator = new NativeJavaImageGenerator();
imageGenerator.SaveAsFile(heatMap.pixelImage, "s3://karl-roth/testScatter", ImageType.PNG);
                                                                                                  