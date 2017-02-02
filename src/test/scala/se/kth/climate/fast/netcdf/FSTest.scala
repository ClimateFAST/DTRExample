package se.kth.climate.fast.netcdf

import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext
import com.google.gson.Gson
import se.kth.climate.fast.common.Metadata
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import ucar.nc2.iosp.netcdf3.N3outputStreamWriter;
import org.apache.spark.SparkConf

class FSTest extends FunSuite with Matchers with SharedSparkContext {

  val path = "hdfs://bbc6.sics.se:26801/FAST/tasminmax_Amon_EC_EARTH_historical_r2i1p1_195001_201212";

  override def conf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.kryo.registrator", "se.kth.climate.fast.netcdf.NCKryoRegistrator").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  test("Metadata should be readable") {
    //    val metaSRDD = sc.textFile("hdfs://bbc6.sics.se:26801/FAST/tasminmax_Amon_EC_EARTH_historical_r2i1p1_195001_201212/metadata.json", 1);
    //    val metaS = metaSRDD.collect().mkString;
    //    println(s"JSON:\n$metaS");
    //    val gson = new Gson();
    //    val meta = gson.fromJson(metaS, classOf[Metadata]);
    //    println(s"Meta:\n$meta");
    implicit val sc = this.sc;
    println(sc.getConf.getAll.mkString("\n"));
    val meta = NetCDF.metaData(path);
    meta.getAttributes should contain key ("title");
  }

  test("Netcdf files should be readable") {
    implicit val sc = this.sc;
    val rdd = NetCDF.rawData(path);
    val titles = rdd.map(ncfile => ncfile.getTitle).collect();
    println("Config: \n" + titles.mkString(";"));
    titles should contain ("tasmax_tasmin_326-488.nc");
    rdd.take(1).foreach(ncfile => {
      println(s"Read: ${ncfile.getTitle}")
    });
  }
}
