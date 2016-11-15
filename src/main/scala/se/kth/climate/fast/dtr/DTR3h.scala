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

object DTR3h extends Logging {
    
    case class VarNames(temp: String, time: String, lat: String, lon: String)
    
    def main(args: Array[String]) {
        val config = new Configuration();

        val conf = new SparkConf().setAppName("Diurnal Temperature Range");
        val sc = new SparkContext(conf);
        implicit val sqlContext = new SQLContext(sc);
        import sqlContext.implicits._

        if (args.length < 6) {
            logError("""
Need path to input file in arg0, 
and save location in arg1, 
tempvar in arg2, 
timevar in arg3, 
latvar in arg4, 
lonvar in arg5!""")
            throw new IllegalArgumentException("Too few arguments!");
        }
        val (metaO, temperatureDf) = FAST.load(args(0), config);
        val outputFileName = args(1);
        val meta = metaO match {
            case Some(metaVal) => sc.broadcast(metaVal)
            case None             => null
        }
        val (dayOf, yearOf, monthOf) = FAST.registerUDFs("time", metaO);
        val varNames = VarNames(args(2), args(3), args(4), args(5));
        // do stuff
        val allRecords = temperatureDf.select(
                yearOf(temperatureDf(varNames.time)) cast IntegerType as "year",
                monthOf(temperatureDf(varNames.time)) cast IntegerType as "month",
                dayOf(temperatureDf(varNames.time)) cast IntegerType as "day",
                temperatureDf(varNames.lat) as "lat",
                temperatureDf(varNames.lon) as "lon",
                temperatureDf(varNames.temp) cast DoubleType as "tas");
        val dayRecords = allRecords.groupBy("day", "month", "lat", "lon").agg(max("tas"), min("tas")).toDF("day", "month", "lat", "lon", "max_tas", "min_tas");
        //val historical = allRecords.where($"year" >= 1970 && $"year" < 2000);
//        val outliers = allRecords.select($"time", 
//                $"year", 
//                $"lat", 
//                $"lon",
//                ($"tasmax" - $"tasmin") as "dtr")
//                .where($"dtr" < 0);
//        outliers.show(Int.MaxValue)
        val monthlyMeans = dayRecords.groupBy("month", "lat", "lon").avg("max_tas", "min_tas").toDF("month", "lat", "lon", "avg_max_tas", "avg_min_tas");
        //val yearlyMeans = historical.groupBy("year", "lat", "lon").avg("tasmax", "tasmin").toDF("year", "lat", "lon", "avg_tasmax", "avg_tasmin");
        //val yearlyDTR = yearlyMeans.select($"year", $"lat", $"lon", ($"avg_tasmax" - $"avg_tasmin") as "avg_dtr");
        val monthlyDTR = monthlyMeans.select($"month", $"lat", $"lon", ($"avg_max_tas" - $"avg_min_tas") as "avg_dtr");
        val dtrmeans = monthlyDTR.groupBy("lat", "lon").avg("avg_dtr").toDF("lat", "lon", "avg_dtr");
        dtrmeans.coalesce(10).write.mode("overwrite").parquet(outputFileName);
    }
}