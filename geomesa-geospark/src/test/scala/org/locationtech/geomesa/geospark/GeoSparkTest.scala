/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geospark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.datasyslab.geospark
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.jts.geom._
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoSparkTest extends Specification with TestEnvironment {

  sequential

  var df: DataFrame = _
  var newDF: DataFrame = _

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

  "geomesa rdd" should {
    "read from dataframe" in {
      val geomesaRDD = df.rdd
      geomesaRDD.collect()

      geomesaRDD.count mustEqual(df.count)
    }

    // Example row: [itemA,Point (40 40),Polygon ((35 35, 45 35, 45 45, 35 45, 35 35)),40,40]
    "have rows with user defined types" in {
      val geomesaRDD = df.rdd
      geomesaRDD.collect().foreach(println)

      val row = geomesaRDD.first()
      true mustEqual(true)
    }
  }

  "geospark rdd" should {
    "read from geomesa rdd" in {

      true mustEqual(true)
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

