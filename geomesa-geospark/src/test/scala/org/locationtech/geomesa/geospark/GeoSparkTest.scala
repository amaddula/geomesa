/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geospark

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.jts.TestEnvironment
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoSparkTest extends Specification with TestEnvironment
                                         with GeoMesaSparkTestEnvironment
                                         with GeoSparkTestEnvironment {

  //note that each mixin trait has different spark sessions and spark contexts
  //TestEnvironment is used to parse the csv into dataframes
  //GeoMesaSparkTestEnvironment is for the tests modeled after the GeoMesaSpatialRDDProvider
  //GeoSparkTestEnvironment is going to be used for managing data in GeoSpark

  sequential

  var df: DataFrame = _
  var newDF: DataFrame = _

  // Must be in form of a function/object not a method
  val simpleFeatureToPoint = (sf: SimpleFeature)  => {
      val pt = sf.getAttribute("point").asInstanceOf[Point]
      pt.setUserData(sf)
      println(pt.getUserData)
      pt
  }

  val simpleFeatureToPolygon = (sf: SimpleFeature)  => {
    val poly = sf.getAttribute("polygon").asInstanceOf[Polygon]
    poly.setUserData(sf)
    println(poly.getUserData)
    poly
  }

  val simpleFeatureToLongLat = (sf: SimpleFeature)  => {
    val geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory(null)
    val lat = sf.getAttribute("latitude").asInstanceOf[Double]
    val long = sf.getAttribute("longitude").asInstanceOf[Double]
    val pt = geometryFactory.createPoint(new Coordinate(long, lat))
    pt.setUserData(sf)
    println("(Long,Lat): " + pt.getUserData)
    pt
  }

  // before
  step {
    val schema = StructType(Array(StructField("name",StringType, nullable=false),
      StructField("pointText", StringType, nullable=false),
      StructField("polygonText", StringType, nullable=false),
      StructField("latitude", DoubleType, nullable=false),
      StructField("longitude", DoubleType, nullable=false)))

    val dataFile = this.getClass.getClassLoader.getResource("jts-example.csv").getPath
    df = spark.read.format("csv")
      .option("delimiter", "\n")
      .option("header", "false")
      .option("sep", "-")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").load(dataFile)
    df.createOrReplaceTempView("inputtable")
    df.show()
  }

  val dsParams = Map("cqengine" -> "true", "geotools" -> "true")

  lazy val jtsExampleSft =
    SimpleFeatureTypes.createType("jtsExample",
    "*point:Point:srid=4326,*polygon:Polygon:srid=4326,latitude:Double,longitude:Double")

  lazy val jtsExampleFeatures: Seq[SimpleFeature] = Seq(
    ScalaSimpleFeature.create(jtsExampleSft, "itemA", "POINT(40 40)", "POLYGON((35 35, 45 35, 45 45, 35 45, 35 35))", 40, 40),
    ScalaSimpleFeature.create(jtsExampleSft, "itemB", "POINT(30 30)", "POLYGON((25 25, 35 25, 35 35, 25 35, 25 25))", 30, 30),
    ScalaSimpleFeature.create(jtsExampleSft, "itemC", "POINT(20 20)", "POLYGON((15 15, 25 15, 25 25, 15 25, 15 15))", 20, 20)
  )

  "geomesa rdd" should {
    "read from dataframe" in {
      val geomesaRDD = df.rdd
      geomesaRDD.collect()
      geomesaRDD.count mustEqual(df.count)
    }

    // Example row: [itemA,Point (40 40),Polygon ((35 35, 45 35, 45 45, 35 45, 35 35)),40,40]
//    "have rows with user defined types" in {
//      val geomesaRDD = df.rdd
//      geomesaRDD.collect().foreach(println)
//      val row = geomesaRDD.first()
//
//      true mustEqual(true)
//    }
  }

  "geospark rdd" should {
    "read from geomesa rdd" in {
      val ds = DataStoreFinder.getDataStore(dsParams)
      ds.createSchema(jtsExampleSft)

      WithClose(ds.getFeatureWriterAppend("jtsExample", Transaction.AUTO_COMMIT)) { writer =>
        jtsExampleFeatures.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val rdd = GeoMesaSpark(dsParams).rdd(new Configuration(), gmsc, dsParams, new Query("jtsExample"))
      true mustEqual(true)
    }
    "should convert to point rdd" in {
      val ds = DataStoreFinder.getDataStore(dsParams)
      ds.createSchema(jtsExampleSft)

      WithClose(ds.getFeatureWriterAppend("jtsExample", Transaction.AUTO_COMMIT)) { writer =>
        jtsExampleFeatures.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val rdd = GeoMesaSpark(dsParams).rdd(new Configuration(), gmsc, dsParams, new Query("jtsExample"))
      val pointRDD = rdd.map(simpleFeatureToPoint)
      pointRDD.collect().foreach(println)

      var isPointRDD = true
      pointRDD.collect().foreach(pt => isPointRDD = (isPointRDD && pt.isInstanceOf[Point]))
      isPointRDD mustEqual(true)
    }
  }

  "spark jts module" should {

    "have rows with user defined types" >> {

//      val params = Map(
//        "geotools" -> "true",
//        "file"     -> "jts-example.csv")
//      val query = new Query("locations")
//      val rdd = GeoMesaSpark(params).rdd(new Configuration(), sc, params, query)

//            spark.sql(
//              """
//                |SELECT *
//                |FROM inputtable
//              """.stripMargin).show()

      //      newDF = spark.sql(
      //        """
      //          |SELECT ST_PointFromText(_c1,' ') AS pointshape
      //          |SELECT ST_PolygonFromText(_c2, ' ') AS polygonshape
      //          |SELECT ST_PointFromText(
      //          |FROM inputtable
      //    """.stripMargin)

      //      newDF = spark.sql(
      //        """
      //          |SELECT ST_Point(cast(inputtable._c3 as Decimal(24,20)),cast(inputtable._c4 as Decimal(24,20))) as shape
      //          |FROM inputtable
      //    """.stripMargin)
      //      newDF.createOrReplaceTempView("newdf")
      //      newDF.show()

      //      newDF = spark.sql(
      //        """
      //          |SELECT ST_GeomFromWKT(_c2) AS polygonshape
      //          |FROM inputtable
      //    """.stripMargin)
      //      newDF.createOrReplaceTempView("newdf")
      //      newDF.show()

      //      JTS VERSION
      //      newDF = df.withColumn("point", st_pointFromText(col("pointText")))
      //        .withColumn("polygon", st_polygonFromText(col("polygonText")))
      //        .withColumn("pointB", st_makePoint(col("latitude"), col("longitude")))
      //
      //      newDF.createOrReplaceTempView("example")
      //      val row = newDF.first()
      //      val gf = new GeometryFactory
      //      row.get(5).isInstanceOf[Point] mustEqual true
      //      row.get(6).isInstanceOf[Polygon] mustEqual true
      //      row.get(7).isInstanceOf[Point] mustEqual true
      true mustEqual(true)
    }

    "create a df from sequence of points" >> {

      //      JTS VERSION
      //      val points = newDF.collect().map{r => r.getAs[Point](5)}
      //      val testDF = spark.createDataset(points).toDF()
      //      testDF.count() mustEqual df.count()
      true mustEqual(true)
    }

    //    "udfs intergrate with dataframe api" >> {
    //      val countSQL = sc.sql("select * from example where st_contains(st_makeBBOX(0.0, 0.0, 90.0, 90.0), point)").count()
    //      val countDF = newDF
    //        .where(st_contains(st_makeBBOX(lit(0.0), lit(0.0), lit(90.0), lit(90.0)), col("point")))
    //        .count()
    //      countSQL mustEqual countDF
    //    }
  }

  // after
  step {
    spark.stop()
  }
}

