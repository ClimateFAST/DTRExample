package se.kth.climate.fast.netcdf

import se.kth.climate.fast.common.Metadata
import se.kth.climate.fast.netcdf.hadoop._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import com.google.gson.Gson
import ucar.nc2.NetcdfFile

object NetCDF extends LazyLogging {
  def metaDataAsync(path: String)(implicit sc: SparkContext, ec: ExecutionContext): Future[Metadata] = {
    Future {
      NetCDF.metaData(path)
    }
  }

  def metaData(path: String)(implicit sc: SparkContext): Metadata = {
    val metaSRDD = sc.textFile(path + "/metadata.json", 1);
    val metaS = metaSRDD.collect().mkString;
    val gson = new Gson();
    gson.fromJson(metaS, classOf[Metadata]);
  }

  def rawData(path: String)(implicit sc: SparkContext): RDD[NetcdfFile] = {
    val rdd = sc.newAPIHadoopFile[Void, NCWriteable, NetCDFFileFormat](path)
    rdd.map {
      case (_, v) => {
        val ncfile = v.get;
        //ncfile.setImmutable(); // can't write them out, so don't let anyone mutate them
        ncfile
      }
    }
  }
}
