package se.kth.climate.fast.math

import breeze.linalg.{ Matrix => BM, DenseMatrix => BDM }
import org.apache.spark.SparkContext

class DenseMatrix(private[math] val mat: BDM[Double]) extends Matrix with Serializable {

    def this(nRows: Int, nCols: Int, values: Array[Double]) {
        this(new BDM(nRows, nCols, values))
    }

    override def numRows(): Long = mat.rows;
    override def numCols(): Long = mat.cols;
    override def foreachActive(f: (Int, Int, Double) => Unit): Unit = {
        mat.foreachPair((key: Tuple2[Int, Int], v: Double) => f(key._1, key._2, v));
    }
    override def transpose(): DenseMatrix = new DenseMatrix(mat.t);

    override def map(f: (Int, Int, Double) => Double): DenseMatrix = {
        val mat2 = mat.mapPairs((index, value) => f(index._1, index._2, value));
        new DenseMatrix(mat2);
    }

    def toLocalBlockMatrix(rowsPerBlock: Int, colsPerBlock: Int): LocalBlockMatrix = {
        require(rowsPerBlock > 0,
            s"rowsPerBlock needs to be greater than 0. rowsPerBlock: $rowsPerBlock")
        require(colsPerBlock > 0,
            s"colsPerBlock needs to be greater than 0. colsPerBlock: $colsPerBlock")
        val m = numRows()
        val n = numCols()
        val numRowBlocks = math.ceil(m.toDouble / rowsPerBlock).toInt
        val numColBlocks = math.ceil(n.toDouble / colsPerBlock).toInt
        val partitioner = GridPartitioner(numRowBlocks, numColBlocks, rowsPerBlock, colsPerBlock)

        val mappedP = mat.mapPairs { (key, value) =>
            val blockRowIndex = (key._1 / rowsPerBlock).toInt
            val blockColIndex = (key._2 / colsPerBlock).toInt

            val rowId = key._1 % rowsPerBlock
            val colId = key._2 % colsPerBlock

            ((blockRowIndex, blockColIndex), (rowId.toInt, colId.toInt, value))
        }.toArray.toList
        val groupedP = mappedP.groupBy((entry) => entry._1).mapValues { v =>
            val mapped = v.map { e => ((e._2._1 -> e._2._2) -> e._2._3) };
            val sorted = mapped.sortBy(x => (x._1._2 -> x._1._1));
            sorted.map(_._2).toArray
        }.toList;
        val blocks: List[((Int, Int), Matrix)] = groupedP.map {
            case ((blockRowIndex, blockColIndex), entries) =>
                val effRows = math.min(m - blockRowIndex.toLong * rowsPerBlock, rowsPerBlock).toInt
                val effCols = math.min(n - blockColIndex.toLong * colsPerBlock, colsPerBlock).toInt
                ((blockRowIndex, blockColIndex), new DenseMatrix(effRows, effCols, entries))
        }
        new LocalBlockMatrix(blocks, rowsPerBlock, colsPerBlock, m, n)
    }

    def toBlockMatrix(rowsPerBlock: Int, colsPerBlock: Int)(implicit sc: SparkContext): BlockMatrix = {
        require(rowsPerBlock > 0,
            s"rowsPerBlock needs to be greater than 0. rowsPerBlock: $rowsPerBlock")
        require(colsPerBlock > 0,
            s"colsPerBlock needs to be greater than 0. colsPerBlock: $colsPerBlock")
        val m = numRows()
        val n = numCols()
        val numRowBlocks = math.ceil(m.toDouble / rowsPerBlock).toInt
        val numColBlocks = math.ceil(n.toDouble / colsPerBlock).toInt
        val partitioner = GridPartitioner(numRowBlocks, numColBlocks, rowsPerBlock, colsPerBlock)

        val mappedP = mat.mapPairs { (key, value) =>
            val blockRowIndex = (key._1 / rowsPerBlock).toInt
            val blockColIndex = (key._2 / colsPerBlock).toInt

            val rowId = key._1 % rowsPerBlock
            val colId = key._2 % colsPerBlock

            ((blockRowIndex, blockColIndex), (rowId.toInt, colId.toInt, value))
        }.toArray.toList
        val groupedP = mappedP.groupBy((entry) => entry._1).mapValues { v =>
            val mapped = v.map { e => ((e._2._1 -> e._2._2) -> e._2._3) };
            val sorted = mapped.sortBy(x => (x._1._2 -> x._1._1));
            sorted.map(_._2).toArray
        }.toList;
        val blocks: List[((Int, Int), Matrix)] = groupedP.map {
            case ((blockRowIndex, blockColIndex), entries) =>
                val effRows = math.min(m - blockRowIndex.toLong * rowsPerBlock, rowsPerBlock).toInt
                val effCols = math.min(n - blockColIndex.toLong * colsPerBlock, colsPerBlock).toInt
                ((blockRowIndex, blockColIndex), new DenseMatrix(effRows, effCols, entries))
        }
        new BlockMatrix(sc.parallelize(blocks), rowsPerBlock, colsPerBlock, m, n)
    }

    override def toString(): String = mat.toString();

    override def toBreeze(): BDM[Double] = mat;

    def scalarMultiply(scalar: Double): DenseMatrix = new DenseMatrix(mat * scalar);
    def scalarAdd(scalar: Double): DenseMatrix = new DenseMatrix(mat + scalar);
    def matrixProductWith(other: DenseMatrix): DenseMatrix = {
        new DenseMatrix(mat * other.mat);
    }

    override def equals(that: Any): Boolean = {
        that match {
            case that: DenseMatrix => mat == that.mat
            case _                 => false
        }
    }

    def mapMasked[M <: MatrixMask](maskF: MaskFactory[M], outputSize: Tuple2[Int, Int],
                                   aligner: Tuple2[Int, Int] => Tuple2[Int, Int], mapper: M => Double): DenseMatrix = {
        val output: BDM[Double] = new BDM(outputSize._1, outputSize._2);

        output.foreachKey { outPos =>
            val inPos = aligner(outPos);
            val mask = maskF.create(outPos, inPos, this);
            val v = mapper(mask);
            output.update(outPos._1, outPos._2, v);
        }

        new DenseMatrix(output);
    }

    protected[math] def apply(i: Int, j: Int): Double = {
        mat(i, j);
    }
}