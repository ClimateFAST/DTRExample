package se.kth.climate.fast.math

import breeze.linalg.{ Matrix => BM, DenseMatrix => BDM }
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkException

class LocalBlockMatrix(
        val blocks: List[((Int, Int), Matrix)],
        val rowsPerBlock: Int,
        val colsPerBlock: Int,
        private var nRows: Long,
        private var nCols: Long) extends Matrix with StrictLogging {

    private type MatrixBlock = ((Int, Int), Matrix) // ((blockRowIndex, blockColIndex), sub-matrix)

    private def logDebug(s: String) = logger.debug(s);
    private def logWarning(s: String) = logger.warn(s);

    override def numRows(): Long = {
        if (nRows <= 0L) estimateDim()
        nRows
    }

    override def numCols(): Long = {
        if (nCols <= 0L) estimateDim()
        nCols
    }

    override def foreachActive(f: (Int, Int, Double) => Unit): Unit = {
        blocks.foreach {
            case ((blockRowIndex, blockColIndex), submat) =>
                val rowOffset = blockRowIndex * rowsPerBlock
                val colOffset = blockColIndex * colsPerBlock
                submat.foreachActive { (i, j, v) =>
                    f(i + rowOffset, j + colOffset, v);
                }
        }
    }

    override def map(f: (Int, Int, Double) => Double): LocalBlockMatrix = {
        val newBlocks = blocks.map {
            case ((blockRowIndex, blockColIndex), submat) =>
                val rowOffset = blockRowIndex * rowsPerBlock
                val colOffset = blockColIndex * colsPerBlock
                val newSubmat = submat.map { (i, j, v) =>
                    f(i + rowOffset, j + colOffset, v);
                }
                ((blockRowIndex, blockColIndex) -> newSubmat)
        }
        new LocalBlockMatrix(newBlocks, rowsPerBlock, colsPerBlock, numRows(), numCols());
    }

    val numRowBlocks = math.ceil(numRows() * 1.0 / rowsPerBlock).toInt

    val numColBlocks = math.ceil(numCols() * 1.0 / colsPerBlock).toInt

    private[math] def createPartitioner(): GridPartitioner =
        GridPartitioner(numRowBlocks, numColBlocks, suggestedNumPartitions = blocks.length)

    private lazy val blockInfo = blocks.map { case (key, block) => (key, (block.numRows, block.numCols)) }

    /** Estimates the dimensions of the matrix. */
    private def estimateDim(): Unit = {
        val (rows, cols) = blockInfo.map {
            case ((blockRowIndex, blockColIndex), (m, n)) =>
                (blockRowIndex.toLong * rowsPerBlock + m,
                    blockColIndex.toLong * colsPerBlock + n)
        }.reduce { (x0, x1) =>
            (math.max(x0._1, x1._1), math.max(x0._2, x1._2))
        }
        if (nRows <= 0L) nRows = rows
        assert(rows <= nRows, s"The number of rows $rows is more than claimed $nRows.")
        if (nCols <= 0L) nCols = cols
        assert(cols <= nCols, s"The number of columns $cols is more than claimed $nCols.")
    }

    /**
      * Validates the block matrix info against the matrix data (`blocks`) and throws an exception if
      * any error is found.
      */
    def validate(): Unit = {
        logDebug("Validating BlockMatrix...")
        // check if the matrix is larger than the claimed dimensions
        estimateDim()
        logDebug("BlockMatrix dimensions are okay...")

        // Check if there are multiple MatrixBlocks with the same index.
        blockInfo.groupBy { case (k, v) => k }.map { case (k, l) => (k, l.length) }.foreach {
            case (key, cnt) =>
                if (cnt > 1) {
                    throw new SparkException(s"Found multiple MatrixBlocks with the indices $key. Please " +
                        "remove blocks with duplicate indices.")
                }
        }
        logDebug("MatrixBlock indices are okay...")
        // Check if each MatrixBlock (except edges) has the dimensions rowsPerBlock x colsPerBlock
        // The first tuple is the index and the second tuple is the dimensions of the MatrixBlock
        val dimensionMsg = s"dimensions different than rowsPerBlock: $rowsPerBlock, and " +
            s"colsPerBlock: $colsPerBlock. Blocks on the right and bottom edges can have smaller " +
            s"dimensions. You may use the repartition method to fix this issue."
        blockInfo.foreach {
            case ((blockRowIndex, blockColIndex), (m, n)) =>
                if ((blockRowIndex < numRowBlocks - 1 && m != rowsPerBlock) ||
                    (blockRowIndex == numRowBlocks - 1 && (m <= 0 || m > rowsPerBlock))) {
                    throw new SparkException(s"The MatrixBlock at ($blockRowIndex, $blockColIndex) has " +
                        dimensionMsg)
                }
                if ((blockColIndex < numColBlocks - 1 && n != colsPerBlock) ||
                    (blockColIndex == numColBlocks - 1 && (n <= 0 || n > colsPerBlock))) {
                    throw new SparkException(s"The MatrixBlock at ($blockRowIndex, $blockColIndex) has " +
                        dimensionMsg)
                }
        }
        logDebug("MatrixBlock dimensions are okay...")
        logDebug("BlockMatrix is valid!")
    }

    /** Collect the distributed matrix on the driver as a `DenseMatrix`. */
    def toLocalMatrix(): Matrix = {
        require(numRows() < Int.MaxValue, "The number of rows of this matrix should be less than " +
            s"Int.MaxValue. Currently numRows: ${numRows()}")
        require(numCols() < Int.MaxValue, "The number of columns of this matrix should be less than " +
            s"Int.MaxValue. Currently numCols: ${numCols()}")
        require(numRows() * numCols() < Int.MaxValue, "The length of the values array must be " +
            s"less than Int.MaxValue. Currently numRows * numCols: ${numRows() * numCols()}")
        val m = numRows().toInt
        val n = numCols().toInt
        val mem = m * n / 125000
        if (mem > 500) logWarning(s"Storing this matrix will require $mem MB of memory!")
        val localBlocks = blocks;
        val values = new Array[Double](m * n)
        localBlocks.foreach {
            case ((blockRowIndex, blockColIndex), submat) =>
                val rowOffset = blockRowIndex * rowsPerBlock
                val colOffset = blockColIndex * colsPerBlock
                submat.foreachActive { (i, j, v) =>
                    val indexOffset = (j + colOffset) * m + rowOffset + i
                    values(indexOffset) = v
                }
        }
        new DenseMatrix(m, n, values)
    }

    /**
      * Transpose this `BlockMatrix`. Returns a new `BlockMatrix` instance sharing the
      * same underlying data. Is a lazy operation.
      */
    def transpose: LocalBlockMatrix = {
        val transposedBlocks = blocks.map {
            case ((blockRowIndex, blockColIndex), mat) =>
                ((blockColIndex, blockRowIndex), mat.transpose)
        }
        new LocalBlockMatrix(transposedBlocks, colsPerBlock, rowsPerBlock, nCols, nRows)
    }

    /** Collects data and assembles a local dense breeze matrix (for test only). */
    def toBreeze(): BDM[Double] = {
        val localMat = toLocalMatrix().asInstanceOf[DenseMatrix]
        localMat.mat
    }

    private[math] def blockMap(
        other: LocalBlockMatrix,
        binMap: (BM[Double], BM[Double]) => BM[Double]): LocalBlockMatrix = {
        require(numRows() == other.numRows(), "Both matrices must have the same number of rows. " +
            s"A.numRows: ${numRows()}, B.numRows: ${other.numRows()}")
        require(numCols() == other.numCols(), "Both matrices must have the same number of columns. " +
            s"A.numCols: ${numCols()}, B.numCols: ${other.numCols()}")
        if (rowsPerBlock == other.rowsPerBlock && colsPerBlock == other.colsPerBlock) {
            val cogrouped = {
                val bGrouped = blocks.groupBy(_._1);
                val obGrouped = other.blocks.groupBy(_._1);
                bGrouped map { g =>
                    val og = obGrouped(g._1);
                    val ogl = if (og != null) og else List.empty[MatrixBlock];
                    val gl = g._2;
                    (g._1 -> (gl.map(_._2), ogl.map(_._2)))
                } toList
            }
            val newBlocks = cogrouped map {
                case ((blockRowIndex, blockColIndex), (a, b)) =>
                    if (a.size > 1 || b.size > 1) {
                        throw new SparkException("There are multiple MatrixBlocks with indices: " +
                            s"($blockRowIndex, $blockColIndex). Please remove them.")
                    }
                    if (a.isEmpty) {
                        val zeroBlock = BM.zeros[Double](b.head.numRows.toInt, b.head.numCols.toInt);
                        val result = binMap(zeroBlock, b.head.toBreeze);
                        new MatrixBlock((blockRowIndex, blockColIndex), Matrices.fromBreeze(result));
                    } else if (b.isEmpty) {
                        new MatrixBlock((blockRowIndex, blockColIndex), a.head);
                    } else {
                        val result = binMap(a.head.toBreeze, b.head.toBreeze)
                        new MatrixBlock((blockRowIndex, blockColIndex), Matrices.fromBreeze(result));
                    }
            }
            new LocalBlockMatrix(newBlocks, rowsPerBlock, colsPerBlock, numRows(), numCols())
        } else {
            throw new SparkException("Cannot perform on matrices with different block dimensions")
        }
    }

    def add(other: LocalBlockMatrix): LocalBlockMatrix =
        blockMap(other, (x: BM[Double], y: BM[Double]) => x + y)

    def subtract(other: LocalBlockMatrix): LocalBlockMatrix =
        blockMap(other, (x: BM[Double], y: BM[Double]) => x - y)

    def multiply(other: LocalBlockMatrix): LocalBlockMatrix = {
        require(numCols() == other.numRows(), "The number of columns of A and the number of rows " +
            s"of B must be equal. A.numCols: ${numCols()}, B.numRows: ${other.numRows()}. If you " +
            "think they should be equal, try setting the dimensions of A and B explicitly while " +
            "initializing them.")
        if (colsPerBlock == other.rowsPerBlock) {
            //            val blockMap = blocks.toMap;
            //            val otherBlockMap = other.blocks.toMap
            //            val blockMatrix = BDM(blockMap.groupBy(_._1._2).map(f));
            val (numRows, numCols) = findDims(blocks);
            val (numRowsOther, numColsOther) = findDims(other.blocks);
            require(numCols == numRowsOther, "Same as above just on block level");
            val resblocks = scala.collection.mutable.Map.empty[(Int, Int), Matrix];
            val s = numCols;
            for (alpha <- 0 to numRows) {
                for (beta <- 0 to numColsOther) {
                    // calcs for result at alpha,beta
                    var Cab = this.blockAt(alpha, 0).toBreeze * other.blockAt(0, beta).toBreeze;
                    for (gamma <- 1 to s) {
                        val Aay = this.blockAt(alpha, gamma);
                        val Byb = other.blockAt(gamma, beta);
                        Cab += Aay.toBreeze() * Byb.toBreeze();
                    }
                    resblocks += ((alpha -> beta) -> new DenseMatrix(Cab));
                }
            }
            new LocalBlockMatrix(resblocks.toList, rowsPerBlock, other.colsPerBlock, this.numRows(), other.numCols());
        } else {
            throw new SparkException("colsPerBlock of A doesn't match rowsPerBlock of B. " +
                s"A.colsPerBlock: $colsPerBlock, B.rowsPerBlock: ${other.rowsPerBlock} -- We don't ahve the implementation for that case, yet")
        }
    }

    def mapMasked[M <: MatrixMask](maskF: MaskFactory[M], outputSize: Tuple2[Int, Int],
                                   aligner: Tuple2[Int, Int] => Tuple2[Int, Int], mapper: M => Double): Matrix = {
        throw new SparkException("Can't be implemented here. Use GhostBlockMatrix instead.");
    }
    
    protected[math] def apply(i: Int, j: Int): Double = {
        throw new SparkException("Not yet implemented!");
    }

    private def blockAt(i: Int, j: Int): Matrix = {
        val blockMap = blocks.toMap
        blockMap((i -> j));
    }

    private def findDims(l: List[MatrixBlock]): (Int, Int) = {
        val is = l.map(_._1._1);
        val js = l.map(_._1._2);
        (is.max, js.max)
    }
}