package se.kth.climate.fast.netcdf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.hadoop.conf.Configuration
import com.typesafe.scalalogging.StrictLogging

object Debug extends App with StrictLogging {
  // val config = new Configuration();

  val conf = new SparkConf().setAppName("Diurnal Temperature Range");
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
  conf.set("spark.kryo.registrator", "se.kth.climate.fast.netcdf.NCKryoRegistrator");
  conf.set("spark.kryoserializer.buffer.max", "512m");
  conf.set("spark.kryo.registrationRequired", "true");
  implicit val sc = new SparkContext(conf);
  sc.setLogLevel("DEBUG");

  val meta = NetCDF.metaData("hdfs://10.0.104.162:8020/Projects/ClimateTest2/TestData/");
  logger.info(s"****** Metadata *********\n$meta");

  val rdd = NetCDF.rawData("hdfs://10.0.104.162:8020/Projects/ClimateTest2/TestData/rows_0-76695843.nc");
  val res = rdd.map { ncfile =>
    val v = ncfile.findVariable("values");
    val arr = v.read();
    val it = arr.getIndexIterator();
    var zeroCount = 0;
    while (it.hasNext) {
      val v = it.getIntNext();
      if (v == 0) zeroCount += 1 else ()
    }
    zeroCount
  }.collect();
  logger.info(s"****** Zeroes *********\n${res.mkString(",")}");
}
