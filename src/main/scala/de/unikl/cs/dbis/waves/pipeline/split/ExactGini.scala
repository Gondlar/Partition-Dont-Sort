package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.pipeline.sort.ExactCardinalities
import de.unikl.cs.dbis.waves.partitions._
import de.unikl.cs.dbis.waves.split.recursive.ObjectCounter
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.schemas._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,sum}
import org.apache.spark.sql.types.DoubleType

case class ExactGini(
  maxBuckets: Int
) extends Recursive[PossibleExactSplit[BucketInfo]] with NoPrerequisites {
  import ExactGini._

  require(maxBuckets > 0)

  private var currentBuckets = 1

  override protected def initialRecursionState(state: PipelineState): PossibleExactSplit[BucketInfo] = {
    currentBuckets = 1
    val info = calculateGini(state.data)
    findBestSplit(state.data, info, Seq.empty).get
  }

  override protected def checkRecursion(recState: PossibleExactSplit[BucketInfo]): Boolean
    = currentBuckets < maxBuckets

  override protected def doRecursionStep(recState: PossibleExactSplit[BucketInfo], df: DataFrame): Seq[PossibleExactSplit[BucketInfo]] = {
    currentBuckets += 1
    recState.info(df).flatMap({ case (df, info, path) =>
      findBestSplit(df, info, path)
    })
  }
}

object ExactGini {
  implicit val ord = Ordering.by[PossibleExactSplit[BucketInfo], Double](_.priority)

  def findBestSplit(df: DataFrame, info: BucketInfo, path: Seq[PartitionTreePath]) = {
    val splits = ObjectCounter.paths(df.schema).flatMap{
      calculateDefinitionLevelSplit(df, info, path, _): Option[PossibleExactSplit[BucketInfo]]
    }
    if (splits.isEmpty) None else Some(splits.max)
  }

  def calculateGini(df: DataFrame): BucketInfo = {
    val size = df.count().toDouble
    require(size > 0)
    val leafPaths = df.schema.leafPaths
    val gini = leafPaths.par.map({ column =>
      1 - df.groupBy(ExactCardinalities.definitionLevel(column))
        .count()
        .select(((col("count")/size)*(col("count")/size)).as("squareProbability"))
        .agg(sum(col("squareProbability")))
        .head()
        .getDouble(0)
    }).sum
    BucketInfo(size, gini)
  }

  def calculateDefinitionLevelSplit(df: DataFrame, info: BucketInfo, path: Seq[PartitionTreePath], key: PathKey) = {
    val present = df.filter(col(key.toSpark).isNotNull)
    val absent = df.filter(col(key.toSpark).isNull)
    try {
      val presentInfo = calculateGini(present)
      val absentInfo = calculateGini(absent)
      val gain = info.gain(presentInfo, absentInfo)
      Some(PossiblePresenceSplit(gain, absentInfo, presentInfo, path, key))
    } catch {
      case e: IllegalArgumentException => None
    }
  }
}

final case class BucketInfo(
  size: Double,
  gini: Double
) {
  def gain(children: BucketInfo*)
    = gini - children.map({ info => (info.size/size)*info.gini }).sum
}

trait PossibleExactSplit[Info] extends RecursionState {
  def info(df: DataFrame): Seq[(DataFrame, Info, Seq[PartitionTreePath])]
}

final case class PossiblePresenceSplit[Info](
  priority: Double,
  absentInfo: Info,
  presentInfo: Info,
  path: Seq[PartitionTreePath],
  key: PathKey
) extends PossibleExactSplit[Info] {

  override def splitShape(df: DataFrame): TreeNode.AnyNode[DataFrame] = SplitByPresence(
    key,
    Bucket(df.filter(col(key.toSpark).isNotNull)),
    Bucket(df.filter(col(key.toSpark).isNull))
  )

  override def info(df: DataFrame): Seq[(DataFrame, Info, Seq[PartitionTreePath])] = {
    val SplitByPresence(_, Bucket(presentDf), Bucket(absentDf)) = splitShape(df)
    Seq((absentDf, absentInfo, path :+ Absent), (presentDf, presentInfo, path :+ Present))
  }
}
