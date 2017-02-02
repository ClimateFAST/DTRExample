package se.kth.climate.fast.math

import breeze.linalg.{ Matrix => BM, DenseMatrix => BDM }
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD

class BlockMatrix(
        val blocks: RDD[((Int, Int), Matrix)],
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

    /**
      * Calls toLocalMatrix first!
      */
    override def foreachActive(f: (Int, Int, Double) => Unit): Unit = {
        val local = toLocalMatrix();
        local.foreachActive(f)
    }

    override def map(f: (Int, Int, Double) => Double): BlockMatrix = {
        val rowsPerBlock_ = this.rowsPerBlock;
        val colsPerBlock_ = this.colsPerBlock;
        val newBlocks = blocks.map {
            case ((blockRowIndex, blockColIndex), submat) =>
                val rowOffset = blockRowIndex * rowsPerBlock_
                val colOffset = blockColIndex * colsPerBlock_
                val newSubmat = submat.map { (i, j, v) =>
                    f(i + rowOffset, j + colOffset, v);
                }
                ((blockRowIndex, blockColIndex) -> newSubmat)
        }
        new BlockMatrix(newBlocks, rowsPerBlock, colsPerBlock, numRows(), numCols());
    }

    val numRowBlocks = math.ceil(numRows() * 1.0 / rowsPerBlock).toInt

    val numColBlocks = math.ceil(numCols() * 1.0 / colsPerBlock).toInt

    private[math] def createPartitioner(): GridPartitioner =
        GridPartitioner(numRowBlocks, numColBlocks, suggestedNumPartitions = blocks.partitions.length)

    private lazy val blockInfo = blocks.map { case (key, block) => (key, (block.numRows, block.numCols)) }.cache()
    private lazy val localBlockInfo = blockInfo.collect()

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
        blockInfo.countByKey().foreach {
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
        val localBlocks = blocks.collect();
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
    def transpose: BlockMatrix = {
        val transposedBlocks = blocks.map {
            case ((blockRowIndex, blockColIndex), mat) =>
                ((blockColIndex, blockRowIndex), mat.transpose)
        }
        new BlockMatrix(transposedBlocks, colsPerBlock, rowsPerBlock, nCols, nRows)
    }

    /** Collects data and assembles a local dense breeze matrix (for test only). */
    def toBreeze(): BDM[Double] = {
        val localMat = toLocalMatrix().asInstanceOf[DenseMatrix]; // Since we created it like this, it must be a DenseMatrix
        localMat.mat
    }

    private[math] def blockMap(
        other: BlockMatrix,
        binMap: (BM[Double], BM[Double]) => BM[Double]): BlockMatrix = {
        require(numRows() == other.numRows(), "Both matrices must have the same number of rows. " +
            s"A.numRows: ${numRows()}, B.numRows: ${other.numRows()}")
        require(numCols() == other.numCols(), "Both matrices must have the same number of columns. " +
            s"A.numCols: ${numCols()}, B.numCols: ${other.numCols()}")
        if (rowsPerBlock == other.rowsPerBlock && colsPerBlock == other.colsPerBlock) {
            val newBlocks = blocks.cogroup(other.blocks, createPartitioner())
                .map {
                    case ((blockRowIndex, blockColIndex), (a, b)) =>
                        if (a.size > 1 || b.size > 1) {
                            throw new SparkException("There are multiple MatrixBlocks with indices: " +
                                s"($blockRowIndex, $blockColIndex). Please remove them.")
                        }
                        if (a.isEmpty) {
                            val zeroBlock = BM.zeros[Double](b.head.numRows.toInt, b.head.numCols.toInt)
                            val result = binMap(zeroBlock, b.head.toBreeze)
                            new MatrixBlock((blockRowIndex, blockColIndex), Matrices.fromBreeze(result))
                        } else if (b.isEmpty) {
                            new MatrixBlock((blockRowIndex, blockColIndex), a.head)
                        } else {
                            val result = binMap(a.head.toBreeze, b.head.toBreeze)
                            new MatrixBlock((blockRowIndex, blockColIndex), Matrices.fromBreeze(result))
                        }
                }
            new BlockMatrix(newBlocks, rowsPerBlock, colsPerBlock, numRows(), numCols())
        } else {
            throw new SparkException("Cannot perform on matrices with different block dimensions")
        }
    }

    def add(other: BlockMatrix): BlockMatrix =
        blockMap(other, (x: BM[Double], y: BM[Double]) => x + y)

    def subtract(other: BlockMatrix): BlockMatrix =
        blockMap(other, (x: BM[Double], y: BM[Double]) => x - y)

    /** Block (i,j) --> Set of destination partitions */
    private type BlockDestinations = Map[(Int, Int), Set[Int]]

    /**
      * Simulate the multiplication with just block indices in order to cut costs on communication,
      * when we are actually shuffling the matrices.
      * The `colsPerBlock` of this matrix must equal the `rowsPerBlock` of `other`.
      * Exposed for tests.
      *
      * @param other The BlockMatrix to multiply
      * @param partitioner The partitioner that will be used for the resulting matrix `C = A * B`
      * @return A tuple of [[BlockDestinations]]. The first element is the Map of the set of partitions
      *         that we need to shuffle each blocks of `this`, and the second element is the Map for
      *         `other`.
      */
    private[math] def simulateMultiply(
        other: BlockMatrix,
        partitioner: GridPartitioner): (BlockDestinations, BlockDestinations) = {
        val leftMatrix = blockInfo.keys.collect() // blockInfo should already be cached
        val rightMatrix = other.blocks.keys.collect()
        val leftDestinations = leftMatrix.map {
            case (rowIndex, colIndex) =>
                val rightCounterparts = rightMatrix.filter(_._1 == colIndex)
                val partitions = rightCounterparts.map(b => partitioner.getPartition((rowIndex, b._2)))
                ((rowIndex, colIndex), partitions.toSet)
        }.toMap
        val rightDestinations = rightMatrix.map {
            case (rowIndex, colIndex) =>
                val leftCounterparts = leftMatrix.filter(_._2 == rowIndex)
                val partitions = leftCounterparts.map(b => partitioner.getPartition((b._1, colIndex)))
                ((rowIndex, colIndex), partitions.toSet)
        }.toMap
        (leftDestinations, rightDestinations)
    }

    /**
      * Left multiplies this [[BlockMatrix]] to `other`, another [[BlockMatrix]]. The `colsPerBlock`
      * of this matrix must equal the `rowsPerBlock` of `other`. If `other` contains
      * [[SparseMatrix]], they will have to be converted to a [[DenseMatrix]]. The output
      * [[BlockMatrix]] will only consist of blocks of [[DenseMatrix]]. This may cause
      * some performance issues until support for multiplying two sparse matrices is added.
      *
      * Note: The behavior of multiply has changed in 1.6.0. `multiply` used to throw an error when
      * there were blocks with duplicate indices. Now, the blocks with duplicate indices will be added
      * with each other.
      */
    def multiply(other: BlockMatrix): BlockMatrix = {
        require(numCols() == other.numRows(), "The number of columns of A and the number of rows " +
            s"of B must be equal. A.numCols: ${numCols()}, B.numRows: ${other.numRows()}. If you " +
            "think they should be equal, try setting the dimensions of A and B explicitly while " +
            "initializing them.")
        if (colsPerBlock == other.rowsPerBlock) {
            val resultPartitioner = GridPartitioner(numRowBlocks, other.numColBlocks,
                math.max(blocks.partitions.length, other.blocks.partitions.length))
            val (leftDestinations, rightDestinations) = simulateMultiply(other, resultPartitioner)
            // Each block of A must be multiplied with the corresponding blocks in the columns of B.
            val flatA = blocks.flatMap {
                case ((blockRowIndex, blockColIndex), block) =>
                    val destinations = leftDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
                    destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
            }
            // Each block of B must be multiplied with the corresponding blocks in each row of A.
            val flatB = other.blocks.flatMap {
                case ((blockRowIndex, blockColIndex), block) =>
                    val destinations = rightDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
                    destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
            }
            val newBlocks = flatA.cogroup(flatB, resultPartitioner).flatMap {
                case (pId, (a, b)) =>
                    a.flatMap {
                        case (leftRowIndex, leftColIndex, leftBlock) =>
                            b.filter(_._1 == leftColIndex).map {
                                case (rightRowIndex, rightColIndex, rightBlock) =>
                                    val C = rightBlock match {
                                        case dense: DenseMatrix => leftBlock.asInstanceOf[DenseMatrix].mat * dense.mat;
                                        //case sparse: SparseMatrix => leftBlock.multiply(sparse.toDense)
                                        case _ =>
                                            throw new SparkException(s"Unrecognized matrix type ${rightBlock.getClass}.")
                                    }
                                    ((leftRowIndex, rightColIndex), C)
                            }
                    }
            }.reduceByKey(resultPartitioner, (a, b) => a + b).mapValues(Matrices.fromBreeze(_).asInstanceOf[Matrix])
            // TODO: Try to use aggregateByKey instead of reduceByKey to get rid of intermediate matrices
            new BlockMatrix(newBlocks, rowsPerBlock, other.colsPerBlock, numRows(), other.numCols())
        } else {
            throw new SparkException("colsPerBlock of A doesn't match rowsPerBlock of B. " +
                s"A.colsPerBlock: $colsPerBlock, B.rowsPerBlock: ${other.rowsPerBlock}")
        }
    }

//    def matrixProductWith(other: BlockMatrix): BlockMatrix = {
//        require(numCols() == other.numRows(), "The number of columns of A and the number of rows " +
//            s"of B must be equal. A.numCols: ${numCols()}, B.numRows: ${other.numRows()}. If you " +
//            "think they should be equal, try setting the dimensions of A and B explicitly while " +
//            "initializing them.");
//        if (colsPerBlock == other.rowsPerBlock) {
//            //            val blockMap = blocks.toMap;
//            //            val otherBlockMap = other.blocks.toMap
//            //            val blockMatrix = BDM(blockMap.groupBy(_._1._2).map(f));
//            val (numRows, numCols) = findDims();
//            val (numRowsOther, numColsOther) = other.findDims();
//            require(numCols == numRowsOther, s"For block multiplication, blockColumns=$numCols must match other.blockRows=$numRowsOther");
//            val s = numCols;
//            val resultPartitioner = GridPartitioner(numRowBlocks, other.numColBlocks,
//                math.max(blocks.partitions.length, other.blocks.partitions.length));
//            val
//            for (alpha <- 0 to numRows) {
//                for (beta <- 0 to numColsOther) {
//                    // calcs for result at alpha,beta
//                    var Cab = this.blockAt(alpha, 0).toBreeze * other.blockAt(0, beta).toBreeze;
//                    for (gamma <- 1 to s) {
//                        val Aay = this.blockAt(alpha, gamma);
//                        val Byb = other.blockAt(gamma, beta);
//                        Cab += Aay.toBreeze() * Byb.toBreeze();
//                    }
//                    resblocks += ((alpha -> beta) -> new DenseMatrix(Cab));
//                }
//            }
//            new LocalBlockMatrix(resblocks.toList, rowsPerBlock, other.colsPerBlock, this.numRows(), other.numCols());
//        } else {
//            throw new SparkException("colsPerBlock of A doesn't match rowsPerBlock of B. " +
//                s"A.colsPerBlock: $colsPerBlock, B.rowsPerBlock: ${other.rowsPerBlock} -- We don't ahve the implementation for that case, yet")
//        }
//    }
//
//    private def findDims(): (Int, Int) = {
//        val is = localBlockInfo.map(_._1._1);
//        val js = localBlockInfo.map(_._1._2);
//        (is.max, js.max)
//    }

    def mapMasked[M <: MatrixMask](maskF: MaskFactory[M], outputSize: Tuple2[Int, Int],
                                   aligner: Tuple2[Int, Int] => Tuple2[Int, Int], mapper: M => Double): Matrix = {
        throw new SparkException("Can't be implemented here. Use GhostBlockMatrix instead.");
    }

    protected[math] def apply(i: Int, j: Int): Double = {
        throw new SparkException("Not yet implemented!");
    }
}