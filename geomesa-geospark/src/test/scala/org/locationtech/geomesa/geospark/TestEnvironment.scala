/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geospark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

/**
 * Common JTS test setup and utilities.
 */
trait TestEnvironment {
  implicit lazy val spark: SparkSession = {
//    JTS VERSION
////    SparkSession.builder()
////      .appName("testSpark")
////      .master("local[*]")
////      .getOrCreate()
////      .withJTS

    SparkSession.builder()
      .appName("testGeoSpark")
      .master("local[*]")
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .master("local[*]").appName("myGeoSparkSQLdemo").getOrCreate()
  }

//  JTS VERSION
//  lazy val sc: SQLContext = spark.sqlContext
//    .withJTS // <-- this should be a noop given the above, but is here to test that code path

val conf = new SparkConf()
conf.setAppName("testGeoSpark")
conf.setMaster("local[*]") // Delete this if run in cluster mode
// Enable GeoSpark custom Kryo serializer
conf.set("spark.serializer", classOf[KryoSerializer].getName)
conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
val sc = new SparkContext(conf)

//  JTS VERSION
//lazy val sc: SQLContext = spark.sqlContext
//  .withJTS // <-- this should be a noop given the above, but is here to test that code path



  /**
   * Constructor for creating a DataFrame with a single row and no columns.
   * Useful for testing the invocation of data constructing UDFs.
   */
//  JTS VERSION
//  def dfBlank(implicit spark: SparkSession): DataFrame = {
//    // This is to enable us to do a single row creation select operation in DataFrame
//    // world. Probably a better/easier way of doing this.
//    spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Row())), StructType(Seq.empty))
//  }
}
