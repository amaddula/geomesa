package org.locationtech.geomesa.geospark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator

trait GeoMesaSparkTestEnvironment {
  // The tests regarding this does not need a spark session - just use gmsc as the context

//  implicit lazy val gmsc: SparkSession = {
//    SparkSession.builder()
//      .appName("testGeoMesaSpark")
//      .master("local[2]")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
//      .master("local[*]").appName("myGeoMesaSparkdemo").getOrCreate()
//  }

  var gmsc: SparkContext = _

  val conf = new SparkConf().setMaster("local[2]").setAppName("testSpark")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
  gmsc = SparkContext.getOrCreate(conf)
}
