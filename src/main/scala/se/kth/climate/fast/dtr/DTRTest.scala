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

object DTRTest extends Logging {
    def main(args: Array[String]) {
        val config = new Configuration();

        val conf = new SparkConf().setAppName("Diurnal Temperature Range");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerAvroSchemas(se.kth.climate.fast.common.Metadata.AVRO);
//        conf.registerKryoClasses(Array(
//                classOf[scala.collection.mutable.WrappedArray$ofRef],
//                classOf[org.apache.spark.sql.types.StructType],
//                classOf[Array[org.apache.spark.sql.types.StructType]],
//                classOf[org.apache.spark.sql.types.StructField],
//                classOf[Array[org.apache.spark.sql.types.StructField]]));
        val sc = new SparkContext(conf);
        implicit val sqlContext = new SQLContext(sc);
        import sqlContext.implicits._

        if (args.length < 4) {
            logError("Need path to tasmax in arg0, tasmin in arg1, results in arg2, and save location in arg3!")
            throw new IllegalArgumentException("Too few arguments!");
        }
        val (maxMetaO, maxDf) = FAST.load(args(0), config);
        val (minMetaO, minDf) = FAST.load(args(1), config);
        val maxMeta = maxMetaO match {
            case Some(maxMetaVal) => sc.broadcast(maxMetaVal)
            case None             => null
        }
        val minMeta = minMetaO match {
            case Some(minMetaVal) => sc.broadcast(minMetaVal)
            case None             => null
        }
        val (dayOf, yearOf, monthOf) = FAST.registerUDFs("time", maxMetaO);
        val results = loadCSV(args(2));
        // do stuff
        val allRecords = joinRecords(maxDf, minDf, yearOf, monthOf);
        val historical = allRecords.where($"year" >= 1970 && $"year" < 2000);
//        val outliers = allRecords.select($"time", 
//                $"year", 
//                $"lat", 
//                $"lon",
//                ($"tasmax" - $"tasmin") as "dtr")
//                .where($"dtr" < 0);
//        outliers.show(Int.MaxValue)
        val monthlyMeans = historical.groupBy("month", "lat", "lon").avg("tasmax", "tasmin").toDF("month", "lat", "lon", "avg_tasmax", "avg_tasmin");
        //val yearlyMeans = historical.groupBy("year", "lat", "lon").avg("tasmax", "tasmin").toDF("year", "lat", "lon", "avg_tasmax", "avg_tasmin");
        //val yearlyDTR = yearlyMeans.select($"year", $"lat", $"lon", ($"avg_tasmax" - $"avg_tasmin") as "avg_dtr");
        val monthlyDTR = monthlyMeans.select($"month", $"lat", $"lon", ($"avg_tasmax" - $"avg_tasmin") as "avg_dtr");
        val dtrmeans = monthlyDTR.groupBy("lat", "lon").avg("avg_dtr").toDF("lat", "lon", "avg_dtr").cache();
        // compare results
        val comp = results.where(results("dtr") !== Double.NaN)
            .join(dtrmeans, "lat" :: "lon" :: Nil)
            .select($"lat",
                $"lon",
                dtrmeans("avg_dtr") as "calc",
                results("dtr") as "correct",
                abs(dtrmeans("avg_dtr") - results("dtr")) as "error")
            .orderBy($"error".desc);
        comp.cache();
        comp.coalesce(1).write.mode("overwrite").parquet(args(3));
        val desc = comp.describe("error").coalesce(1);
        desc.show();
        val textPath = s"${args(3)}.description.txt";
        desc.write.mode("overwrite").json(textPath);
    }

    def joinRecords(maxDf: DataFrame, minDf: DataFrame, yearOf: UserDefinedFunction, monthOf: UserDefinedFunction): DataFrame = {
        maxDf.join(minDf, Seq("time", "lat", "lon"))
            .select(maxDf("time") as "time",
                yearOf(maxDf("time")) cast IntegerType as "year",
                monthOf(maxDf("time")) cast IntegerType as "month",
                maxDf("lat") as "lat",
                (((maxDf("lon") + 180.0) % 360.0) - 180.0) as "lon",
                maxDf("tasmax") cast DoubleType as "tasmax",
                minDf("tasmin") cast DoubleType as "tasmin")
    }

    def loadCSV(path: String)(implicit sqlContext: SQLContext): DataFrame = {
        val customSchema = StructType(
            StructField("lat", DoubleType, false) ::
                StructField("lon", DoubleType, false) ::
                StructField("dtr", DoubleType, false) :: Nil);
        sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "false") // Use first line of all files as header
            .schema(customSchema) // Automatically infer data types
            .load(path)
    }
}