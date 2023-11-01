package de.unikl.cs.dbis.waves.testjobs

import de.unikl.cs.dbis.waves.partitions._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.visitors.PartitionTreeVisitor
import de.unikl.cs.dbis.waves.partitions.visitors.SingleResultVisitor
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.PartitionFolder

import org.apache.hadoop.fs.Path
import java.nio.charset.StandardCharsets
import de.unikl.cs.dbis.waves.pipeline.PipelineState
import de.unikl.cs.dbis.waves.pipeline.util.CalculateVersionTree
import de.unikl.cs.dbis.waves.pipeline.util.CalculateTotalFingerprint
import de.unikl.cs.dbis.waves.pipeline.StructureMetadata
import de.unikl.cs.dbis.waves.util.VersionTree
import de.unikl.cs.dbis.waves.util.TotalFingerprint
import de.unikl.cs.dbis.waves.pipeline.split.ModelGini

object FindBadSplits {
  type NamedTreePath = Seq[(PartitionTreePath, PathKey)]

  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession(s"FindBadSplits ${jobConfig.wavesPath}")

    // load partition tree
    val hdfs = PartitionTreeHDFSInterface(spark, jobConfig.wavesPath)
    implicit val fs = hdfs.fs

    // get partition size information
    val bucketSizesBuilder = Seq.newBuilder[(String, Long, Long)]
    val tree = hdfs.read().get
    val sizeMetadata = tree.root.map { (name, index) =>
      val folder = new PartitionFolder(jobConfig.wavesPath, name, false)
      if (folder.isEmpty) {
        bucketSizesBuilder += ((name, 0L, 0L))
        None
      } else {
        val diskSize = folder.diskSize
        val rowCount = spark.read.parquet(folder.filename).count()
        bucketSizesBuilder += ((name, diskSize, rowCount))
        Some((diskSize, rowCount))
      }
    }
    val bucketSizes = bucketSizesBuilder.result()

    // get VersionTree
    val versionTree = StructureMetadata(CalculateTotalFingerprint(PipelineState(spark.read.json(jobConfig.inputPath), ""))).asInstanceOf[TotalFingerprint]

    // analyze partition tree
    val visitor = new PartitionTreeVisitor[Option[(Long, Long)]] with SingleResultVisitor[Option[(Long, Long)],(Seq[NamedTreePath],Seq[(NamedTreePath, Double)],Seq[(NamedTreePath, Double)],Seq[(Double, Long, Long)])] {
      var currentNodeMetadata: Option[(Long, Long)] = None
      var currentNodeStructure = versionTree
      var knownEmpty: Seq[NamedTreePath] = Seq.empty
      var sizeErrors: Seq[(NamedTreePath, Double)] = Seq.empty
      var rowsErrors: Seq[(NamedTreePath, Double)] = Seq.empty
      var bucketErrors: Seq[(Double, Long, Long)] = Seq.empty

      override def visit(bucket: Bucket[Option[(Long, Long)]]): Unit = {
        currentNodeMetadata = bucket.data

        val estimatedRows = currentNodeStructure.asInstanceOf[TotalFingerprint].total
        val actualRows = currentNodeMetadata.map(_._2).getOrElse(0L)
        val error = (estimatedRows-actualRows).abs.toDouble / actualRows
        bucketErrors = bucketErrors :+ (error, actualRows, estimatedRows)
      }

      override def visit(node: SplitByPresence[Option[(Long, Long)]]): Unit = {
        val expectedRatio = currentNodeStructure.absoluteProbability(node.key)
        val (absentStructure, presentStructure) = currentNodeStructure.splitBy(node.key).right.getOrElse{
          val emptySide = TotalFingerprint.empty(currentNodeStructure.names, currentNodeStructure.leafs)
          if (expectedRatio == 0) (currentNodeStructure, emptySide) else (emptySide, currentNodeStructure)
        }

        currentNodeStructure = absentStructure
        node.absentKey.accept(this)
        val absentMetadata = currentNodeMetadata
        val absentKnown = extendKnownList(Absent, node.key, knownEmpty)
        knownEmpty = Seq.empty
        val absentSizeErrors = extendErrorList(Absent, node.key, sizeErrors)
        sizeErrors = Seq.empty
        val absentRowsErrors = extendErrorList(Absent, node.key, rowsErrors)
        rowsErrors = Seq.empty

        currentNodeStructure = presentStructure
        node.presentKey.accept(this)
        val presentMetadata = currentNodeMetadata
        val presentKnown = extendKnownList(Present, node.key, knownEmpty)
        val presentSizeErrors = extendErrorList(Present, node.key, sizeErrors)
        val presentRowsErrors = extendErrorList(Present, node.key, rowsErrors)

        val newKnown = (absentMetadata, presentMetadata) match {
          case (None, None) => Seq.empty
          case (None, _) => Seq(Seq((Absent, node.key)))
          case (_, None) => Seq(Seq((Present, node.key)))
          case (_, _) => Seq.empty
        }
        currentNodeMetadata = mergeSubtrees(absentMetadata, presentMetadata)
        knownEmpty = newKnown ++ absentKnown ++ presentKnown
        val (newSizeError, newRowsError) = newErrors(currentNodeMetadata, presentMetadata, absentMetadata, expectedRatio)
        sizeErrors = newSizeError ++ absentSizeErrors ++ presentSizeErrors
        rowsErrors = newRowsError ++ absentRowsErrors ++ presentRowsErrors
      }

      override def visit(node: SplitByValue[Option[(Long, Long)]]): Unit = {
        val expectedRatio = currentNodeStructure.absoluteProbability(node.key) * currentNodeStructure.separatorForLeaf(Some(node.key), .5).right.get._2
        val (lessStrcture, moreStructure) = currentNodeStructure.splitBy(Some(node.key), .5).right.getOrElse{
          val emptySide = TotalFingerprint.empty(currentNodeStructure.names, currentNodeStructure.leafs)
          if (expectedRatio == 0) (emptySide, currentNodeStructure) else (currentNodeStructure, emptySide)
        }

        currentNodeStructure = lessStrcture
        node.less.accept(this)
        val lessMetadata = currentNodeMetadata
        val lessKnown = extendKnownList(Less, node.key, knownEmpty)
        knownEmpty = Seq.empty
        val lessSizeErrors = extendErrorList(Less, node.key, sizeErrors)
        sizeErrors = Seq.empty
        val lessRowsErrors = extendErrorList(MoreOrNull, node.key, rowsErrors)
        rowsErrors = Seq.empty

        currentNodeStructure = moreStructure
        node.more.accept(this)
        val moreMetadata = currentNodeMetadata
        val moreKnown = extendKnownList(MoreOrNull, node.key, knownEmpty)
        val moreSizeErrors = extendErrorList(MoreOrNull, node.key, sizeErrors)
        val moreRowsErrors = extendErrorList(MoreOrNull, node.key, rowsErrors)

        val newKnown = (lessMetadata, moreMetadata) match {
          case (None, None) => Seq.empty
          case (None, _) => Seq(Seq((Less, node.key)))
          case (_, None) => Seq(Seq((MoreOrNull, node.key)))
          case _ => Seq.empty
        }
        currentNodeMetadata = mergeSubtrees(lessMetadata, moreMetadata)
        knownEmpty = newKnown ++ lessKnown ++ moreKnown
        val (newSizeError, newRowsError) = newErrors(currentNodeMetadata, lessMetadata, moreMetadata, expectedRatio)
        sizeErrors = newSizeError ++ lessSizeErrors ++ moreSizeErrors
        rowsErrors = newRowsError ++ lessRowsErrors ++ moreRowsErrors
      }

      override def visit(root: Spill[Option[(Long, Long)]]): Unit = ???

      override def visit(nway: EvenNWay[Option[(Long, Long)]]): Unit = {
        assert(nway.children.forall(_.isInstanceOf[Bucket[Option[(Long, Long)]]]))

        val estimatedRows = currentNodeStructure.asInstanceOf[TotalFingerprint].total / nway.size
        val childMetadata = for (child <- nway.children) yield {
          val childMetadata = child.asInstanceOf[Bucket[Option[(Long, Long)]]].data
          val actualRows = childMetadata.map(_._2).getOrElse(0L)
          val error = (estimatedRows-actualRows).abs.toDouble / actualRows
          bucketErrors = bucketErrors :+ (error, actualRows, estimatedRows)
          childMetadata
        }

        val existingChildMetadata = childMetadata.flatten
        if (existingChildMetadata.isEmpty) {
          currentNodeMetadata = None
        } else {
          currentNodeMetadata = Some(existingChildMetadata.fold((0L, 0L))((lhs, rhs) => (lhs._1 + rhs._1, lhs._2 + rhs._2)))
        }
      }

      override def result: (Seq[NamedTreePath],Seq[(NamedTreePath, Double)],Seq[(NamedTreePath, Double)],Seq[(Double, Long, Long)])
        = (knownEmpty, sizeErrors, rowsErrors, bucketErrors)

      private def mergeSubtrees(lhs: Option[(Long, Long)], rhs: Option[(Long, Long)])
        = ModelGini.mergeOptions[(Long, Long)]({
          case ((lhsSize, lhsRows), (rhsSize, rhsRows)) => (lhsSize+rhsSize, lhsRows+rhsRows)
        })(lhs, rhs)

      private def extendKnownList(step: PartitionTreePath, key: PathKey, list: Seq[NamedTreePath])
        = for (path <- list) yield (step, key) +: path

      private def error(total: Long, part: Long, expected: Double): Double
        = ((part / total.toDouble) - expected).abs

      private def extendErrorList(step: PartitionTreePath, key: PathKey, list: Seq[(NamedTreePath, Double)])
        = for ((path, error) <- list) yield ((step, key) +: path, error)

      private def newErrors(total: Option[(Long, Long)], subjectSide: Option[(Long, Long)], otherSide: Option[(Long, Long)], expectedRatio: Double) = (for {
          (actualSize, actualRows) <- total
          if otherSide.isDefined
          (subjectSize, subjectRows) <- subjectSide
         } yield (
          (Seq.empty, error(actualSize, subjectSize, expectedRatio)),
          (Seq.empty, error(actualRows, subjectRows, expectedRatio))
         )).toSeq.unzip
    }
    
    val (empty, sizeError, rowsError, bucketError) = sizeMetadata(visitor)
    val sb = new StringBuilder()
    sb ++= "Partition Sizes:\n"
    sb ++= "Name, Bytes, Rows\n"
    sb ++= bucketSizes.sortBy(_._3).map(t => s"${t._1}, ${t._2}, ${t._3}").reverse.mkString("\n")
    sb ++= "\n\nEmpty Partitions:\n"
    sb ++= empty.sortBy(_.size).reverse.mkString("\n")
    sb ++= "\n\nError by Size:\n"
    sb ++= sizeError.sortBy(_._2).reverse.mkString("\n")
    sb ++= "\n\nError by Rows:\n"
    sb ++= rowsError.sortBy(_._2).reverse.mkString("\n")
    sb ++= "\n\nError by Buckets:\n"
    sb ++= "Actual, Estimated, Error\n"
    sb ++= bucketError.sortBy(_._2).reverse.map(t => s"${t._2}, ${t._3}, ${t._1}").mkString("\n")

    val path = new Path(s"badPaths.txt")
    val out = fs.create(path)
    out.write(sb.toString.getBytes(StandardCharsets.UTF_8))
    out.close()
  }
}
