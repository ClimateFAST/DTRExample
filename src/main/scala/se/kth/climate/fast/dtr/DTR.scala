package se.kth.climate.fast.dtr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.UserDefinedFunction

object DTR extends Logging {

    case class VarNames(tmin: String, tmax: String, time: String, lat: String, lon: String)

    def main(args: Array[String]) {
        val config = new Configuration();

        val conf = new SparkConf().setAppName("Diurnal Temperature Range");
        val sc = new SparkContext(conf);
        implicit val sqlContext = new SQLContext(sc);
        import sqlContext.implicits._

        if (args.length < 8) {
            logError("""
Need path to tasmax file in arg0, 
tasmin file in arg1, 
and save location in arg2, 
minvar in arg3, 
maxvar in arg4, 
timevar in arg5, 
latvar in arg6, 
lonvar in arg7!""")
            throw new IllegalArgumentException("Too few arguments!");
        }
        val (maxMetaO, maxDf) = FAST.load(args(0), config);
        val (minMetaO, minDf) = FAST.load(args(1), config);
        val outputFileName = args(2);
        val maxMeta = maxMetaO match {
            case Some(maxMetaVal) => sc.broadcast(maxMetaVal)
            case None             => null
        }
        val minMeta = minMetaO match {
            case Some(minMetaVal) => sc.broadcast(minMetaVal)
            case None             => null
        }
        val (dayOf, yearOf, monthOf) = FAST.registerUDFs("time", maxMetaO);
        val varNames = VarNames(args(3), args(4), args(5), args(6), args(7));
        // do stuff
        val allRecords = joinRecords(maxDf, minDf, yearOf, monthOf, varNames);
        //val historical = allRecords.where($"year" >= 1970 && $"year" < 2000);
        //        val outliers = allRecords.select($"time", 
        //                $"year", 
        //                $"lat", 
        //                $"lon",
        //                ($"tasmax" - $"tasmin") as "dtr")
        //                .where($"dtr" < 0);
        //        outliers.show(Int.MaxValue)
        val monthlyMeans = allRecords.groupBy("month", "lat", "lon").avg("tasmax", "tasmin").toDF("month", "lat", "lon", "avg_tasmax", "avg_tasmin");
        //val yearlyMeans = historical.groupBy("year", "lat", "lon").avg("tasmax", "tasmin").toDF("year", "lat", "lon", "avg_tasmax", "avg_tasmin");
        //val yearlyDTR = yearlyMeans.select($"year", $"lat", $"lon", ($"avg_tasmax" - $"avg_tasmin") as "avg_dtr");
        val monthlyDTR = monthlyMeans.select($"month", $"lat", $"lon", ($"avg_tasmax" - $"avg_tasmin") as "avg_dtr");
        val dtrmeans = monthlyDTR.groupBy("lat", "lon").avg("avg_dtr").toDF("lat", "lon", "avg_dtr");
        dtrmeans.coalesce(10).write.mode("overwrite").parquet(outputFileName);
    }

    def joinRecords(maxDf: DataFrame, minDf: DataFrame, yearOf: UserDefinedFunction, monthOf: UserDefinedFunction, varNames: VarNames): DataFrame = {
        maxDf.join(minDf, Seq(varNames.time, varNames.lat, varNames.lon))
            .select(maxDf(varNames.time) as "time",
                yearOf(maxDf(varNames.time)) cast IntegerType as "year",
                monthOf(maxDf(varNames.time)) cast IntegerType as "month",
                maxDf(varNames.lat) as "lat",
                maxDf(varNames.lon) as "lon",
                maxDf(varNames.tmax) cast DoubleType as "tasmax",
                minDf(varNames.tmin) cast DoubleType as "tasmin")
    }
}