package se.kth.climate.fast.math

import breeze.linalg.{ Matrix => BM, DenseMatrix => BDM }

trait Matrix {
    def numRows(): Long;
    def numCols(): Long;
    def foreachActive(f: (Int, Int, Double) => Unit): Unit;
    def transpose(): Matrix;
    def toBreeze(): BDM[Double];
    def map(f: (Int, Int, Double) => Double): Matrix;
    def mapValues(f: Double => Double): Matrix = {
        map((i, j, v) => f(v));
    }
    def mapMasked[M <: MatrixMask](maskF: MaskFactory[M], outputSize: Tuple2[Int, Int],
                                   aligner: Tuple2[Int, Int] => Tuple2[Int, Int], mapper: M => Double): Matrix;
    protected[math] def apply(i: Int, j: Int): Double;
}

object Matrices {
    def fromBreeze(mat: BM[Double]): DenseMatrix = {
        new DenseMatrix(mat.toDenseMatrix)
    }
}

trait MaskFactory[M <: MatrixMask] {
    def create(pos: Tuple2[Int, Int], offset: Tuple2[Int, Int], matrix: Matrix): M;
}

trait MatrixMask {
    def numRows(): Int;
    def numCols(): Int;
    def apply(i: Int, j: Int): Double;
    def i: Int;
    def j: Int;
}

class ScalarMask(pos: Tuple2[Int, Int], v: Double) extends MatrixMask {
    def numRows(): Int = 1;
    def numCols(): Int = 1;
    def apply(i: Int, j: Int): Double = v;
    def i: Int = pos._1;
    def j: Int = pos._2;
}

object ScalarMask extends MaskFactory[ScalarMask] {
    def create(pos: Tuple2[Int, Int], offset: Tuple2[Int, Int], matrix: Matrix): ScalarMask = {
        new ScalarMask(pos, matrix(offset._1, offset._2))
    }
}

class SquareMask(pos: Tuple2[Int, Int], val q00: Double, val q01: Double, val q10: Double, val q11: Double) extends MatrixMask {
    def numRows(): Int = 2;
    def numCols(): Int = 2;
    def apply(i: Int, j: Int): Double = {
        if (i == 0 && j == 0) q00
        else if (i == 0 && j == 1) q01
        else if (i == 1 && j == 0) q10
        else if (i == 1 && j == 1) q11
        else throw new IndexOutOfBoundsException(s"At ${(i, j)}");
    }
    def i: Int = pos._1;
    def j: Int = pos._2;
}

case class SquareMaskF(noValueExtent: Double => Double) extends MaskFactory[SquareMask] {
    def create(pos: Tuple2[Int, Int], offset: Tuple2[Int, Int], matrix: Matrix): SquareMask = {
        val q00 = matrix(offset._1, offset._2);
        val colsMinus1 = matrix.numCols() - 1;
        val rowsMinus1 = matrix.numRows() - 1;
        val q10 = if (offset._2 < colsMinus1) matrix(offset._1, offset._2 + 1) else noValueExtent(q00);
        val q01 = if (offset._1 < rowsMinus1) matrix(offset._1 + 1, offset._2) else noValueExtent(q00);
        val q11 = if (offset._1 < rowsMinus1 && offset._2 < colsMinus1) matrix(offset._1 + 1, offset._2 + 1) else noValueExtent(q00);
        new SquareMask(pos, q00, q01, q10, q11);
    }
}

object Aligner {
    def id(outPos: Tuple2[Int, Int]): Tuple2[Int, Int] = outPos;
}

object Mapper {
    def id[M <: MatrixMask](mask: M): Double = mask(0, 0);
}

object MaskOps {
    def interpolate(m: Matrix, targetSize: Tuple2[Int, Int]): Matrix = {
        val rowScale = (m.numRows() - 1).toDouble / (targetSize._1 - 1).toDouble;
        val colScale = (m.numCols() - 1).toDouble / (targetSize._2 - 1).toDouble;
        if (rowScale > 1 || colScale > 1) {
            println("Downscaling will alias heavily and not interpolate!");
        }
        println(s"Rescaling rows with $rowScale and cols with $colScale");
        val rowRescale = (outRow: Int) => ((outRow) * rowScale);
        val colRescale = (outCol: Int) => ((outCol) * colScale);
        val aligner = (outPos: Tuple2[Int, Int]) => {
            (Math.floor(rowRescale(outPos._1)).toInt, Math.floor(colRescale(outPos._2)).toInt).swap
        }
        m.mapMasked(SquareMaskF((x) => x), targetSize, aligner, (mask: SquareMask) => {
            val xS = rowRescale(mask.i);
            val yS = colRescale(mask.j);
            val x = xS - Math.floor(xS);
            val y = yS - Math.floor(yS);
            val xI = 1-x;
            val yI = 1-y;
            val v = mask.q00*xI*yI+mask.q01*xI*y+mask.q10*x*yI+mask.q11*x*y;
            println(s"Mapped ($x (${mask.i}), $y (${mask.j})) -> {{${mask.q00}, ${mask.q01}}, {${mask.q10}, ${mask.q11}}} => $v");
            v
        })
    }
}

