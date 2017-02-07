package se.kth.climate.fast.netcdf

import org.scalatest._;
import com.holdenkarau.spark.testing.SharedSparkContext;
import com.google.gson.Gson;
import se.kth.climate.fast.common.Metadata;
import se.kth.climate.fast.netcdf.testing.FileGenerator;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import com.google.common.io.Files;
import ucar.nc2.iosp.netcdf3.N3outputStreamWriter;
import org.apache.spark.SparkConf;
import scala.collection.JavaConversions._;
import org.apache.hadoop.fs.Path;

class FSTest extends FunSuite with Matchers with SharedSparkContext with BeforeAndAfter {

  val FSIZE = 200l * 1024l * 1024l;

  override def conf = {
    new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", appID).
      set("spark.kryo.registrator", "se.kth.climate.fast.netcdf.NCKryoRegistrator").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  private var path: String = null;
  private var fgen: FileGenerator = null;
  val gson = new Gson();

  before {
    val dir = Files.createTempDir();
    dir.deleteOnExit();
    path = dir.getAbsolutePath;
    fgen = new FileGenerator(FSIZE);
    val files = fgen.generateBlocks(dir, 7);
    files.foreach(p => {
      val f = p.toFile();
      f.deleteOnExit();
    });
    val meta = fgen.generateMeta();
    val metaOut = new File(path + "/metadata.json");
    val writer = new NetCDFWriter();
    val q = new WorkQueue[File](1);
    writer.writeMeta(meta, q);
    val f = q.take().get(); // I know it must be in there due to sync write
    Files.move(f, metaOut);
    metaOut.deleteOnExit()
    println(s"Generated files: ${files.mkString(",")}, $metaOut");
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
    fgen.checkMeta(meta) should be (true);
  }

  test("Netcdf files should have right content") {
    implicit val sc = this.sc;
    val rdd = NetCDF.rawData(path);
    val sum = rdd.map(ncfile => {
      val cs = FileGenerator.sumBlock(ncfile);
      if (cs.getValue1() > -1) {
        ("test" -> (cs.getValue0.asInstanceOf[Int] -> cs.getValue1.asInstanceOf[Long]))
      } else {
        println("Invalid mapper result < 0! Ignoring record.");
        ("fail" -> (0 -> 0l))
      }
    }).reduceByKey({ case ((c1, s1), (c2, s2)) => (c1 + c2, s1 + s2) });
    val res = sum.collect().filter({ case (k, v) => k.equals("test") }).apply(0);
    val (c, s) = res._2;
    fgen.checkBlockSum(s, c) should be (true);
  }
}
