package se.kth.climate.fast.math

import org.scalatest._
import breeze.linalg.{ Matrix => BM, DenseMatrix => BDM }
import java.text.DecimalFormat


class DenseMatrixTest extends FunSuite with Matchers {

    val mat9 = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0);

    test("Grid should be laid out properly") {
        val dm = new DenseMatrix(3, 3, mat9);
        println(dm);
        val dm2 = new DenseMatrix(BDM.zeros(9, 9));
        println(dm2);
    }

    test("Matrix Operations") {
        val dm = new DenseMatrix(3, 3, mat9);
        val prod = dm.matrixProductWith(dm.transpose());
        val prodM = toMathematica(prod);
        prodM shouldBe "{{66.0,78.0,90.0},{78.0,93.0,108.0},{90.0,108.0,126.0}}";
        println("dm.dmT: " + prodM);
    }

    test("Nonlinear Matrix Operations") {
        val dm = new DenseMatrix(3, 3, mat9);
        val dmMapped = dm.mapValues(x => x * x);
        val dmSquared = new DenseMatrix(3, 3, mat9.map(x => x * x));
        dmMapped shouldBe dmSquared;
    }

    test("Masked identity") {
        val dm = new DenseMatrix(3, 3, mat9);
        val dmId = dm.mapMasked(ScalarMask, (3, 3), Aligner.id, Mapper.id);
        dmId shouldBe dm;
    }

    test("Interpolate Up") {
        val dm2x2 = new DenseMatrix(2, 2, Array(1.0, 0.0, 0.0, 0.0));
        val dm3x3 = MaskOps.interpolate(dm2x2, (3, 3));
        val dm3x3M = toMathematica(dm3x3);
        println("dm3x3: " + dm3x3M);
        dm3x3M shouldBe "{{1.0,0.5,0.0},{0.5,0.25,0.0},{0.0,0.0,0.0}}";
        val dm = new DenseMatrix(3, 3, mat9);
        val dm9x9 = MaskOps.interpolate(dm, (9, 9));
        val dm9x9M = toMathematica(dm9x9);
        println("dm9x9: " + toMathematica(dm9x9));
        dm9x9M shouldBe "{{1.0,1.25,1.5,1.75,2.0,2.25,2.5,2.75,3.0},{1.75,2.0,2.25,2.5,2.75,3.0,3.25,3.5,3.75},{2.5,2.75,3.0,3.25,3.5,3.75,4.0,4.25,4.5},{3.25,3.5,3.75,4.0,4.25,4.5,4.75,5.0,5.25},{4.0,4.25,4.5,4.75,5.0,5.25,5.5,5.75,6.0},{4.75,5.0,5.25,5.5,5.75,6.0,6.25,6.5,6.75},{5.5,5.75,6.0,6.25,6.5,6.75,7.0,7.25,7.5},{6.25,6.5,6.75,7.0,7.25,7.5,7.75,8.0,8.25},{7.0,7.25,7.5,7.75,8.0,8.25,8.5,8.75,9.0}}";
    }

    test("Interpolate Down") {
        val dm6x6 = new DenseMatrix(6, 6, (1 to 36).toArray.map(_.toDouble));
        println("dm6x6: " + dm6x6);
        val dm4x4 = MaskOps.interpolate(dm6x6, (4, 4));
        val dm4x4M = toMathematica(dm4x4);
        println("dm4x4: " + dm4x4M);
        dm4x4M shouldBe "{{1.0,2.66667,4.33333,6.0},{11.0,12.66667,14.33333,16.0},{21.0,22.66667,24.33333,26.0},{31.0,32.66667,34.33333,36.0}}";
    }

    private val dFormat = new DecimalFormat("#####################0.0####");
    
    private def toMathematica(m: Matrix): String = {
        val rows: Array[Array[Double]] = Array.ofDim(m.numRows().toInt);
        m.foreachActive((i, j, v) => {
            var row = rows(i);
            if (row == null) {
                row = Array.ofDim(m.numCols().toInt);
                rows(i) = row;
            }
            row(j) = v;
        });
        val sb = new scala.collection.mutable.StringBuilder();
        sb.append('{');
        for (row <- rows) {
            sb.append('{');
            for (col <- row) {
                sb.append(dFormat.format(col));
                sb.append(',');
            }
            sb.deleteCharAt(sb.length - 1); // delete extra ,
            sb.append('}');
            sb.append(',');
        }
        sb.deleteCharAt(sb.length - 1); // delete extra , 
        sb.append('}');
        sb.result()
    }
}