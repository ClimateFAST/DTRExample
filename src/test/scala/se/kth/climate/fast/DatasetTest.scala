package se.kth.climate.fast

import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext;
import se.kth.climate.fast.metadata._

class DatasetTest extends FunSuite with SharedSparkContext with Matchers {

  //  object TestData extends Metadata {
  //    val rows = d[Int](10);
  //    //val c = const[Int]
  //    val data = v[Int] indexedBy (rows);
  //    //def * = Tuple1(c);
  //  }

  test("Something") {
    //    import TestData._
    //
    //    val ds = Dataset.scalar(c := 5);
    //    val ds2 = ds.map

  }

}
