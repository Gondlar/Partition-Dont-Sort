package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.nested.schemas._
import de.unikl.cs.dbis.waves.util.nested.DataTypeVisitor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{
  when, min, max, approx_count_distinct, array, struct
}
import org.apache.spark.sql.types.{StructType, DataType, ArrayType, MapType}

import de.unikl.cs.dbis.waves.util.{VersionTree, Leaf, Versions}
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.SingleResult
import de.unikl.cs.dbis.waves.util.ColumnValue
import de.unikl.cs.dbis.waves.split.recursive.ColumnMetadata
import de.unikl.cs.dbis.waves.util.operators.TempColumn
import de.unikl.cs.dbis.waves.util.operators.collect_set_with_count

/**
  * Set the StructureMetadata field of the Pipeline State to a VersionTree
  * calculated from the state's data.
  */
object CalculateVersionTree extends PipelineStep with NoPrerequisites {

  override def run(state: PipelineState): PipelineState = {
    val tree = fromDataFrame(state.data, Schema(state))
    StructureMetadata(state) = tree
  }

  /**
    * Construct a VersionTree which holds the metadata of a given DataFrame and
    * schema. This method will perform two scans on the DataFrame
    *
    * @param df the DataFrame to process
    * @param schema the schema of the df
    * @return the constructed VersionTree
    */
  def fromDataFrame(df: DataFrame, schema: StructType): VersionTree = {
    val (features, aggregates) = schema(CollectColumnsVisitor())
    val info = df.select(features:_*).agg(aggregates.head, aggregates.tail:_*).collect()(0)
    schema(MakeVersionedStructureVisitor(info))
  }

  final case class CollectColumnsVisitor()
  extends DataTypeVisitor with SingleResult[(Seq[Column],Seq[Column])] {
    var currentPath: Option[PathKey] = None
    val aggregates = Seq.newBuilder[Column]
    val features = Seq.newBuilder[Column]

    override def visitStruct(row: StructType): Unit = {
      val name = currentPath.map(_.toDotfreeString).getOrElse("root")
      val sortedChildren = row.fields.sortBy(_.name)
      
      val structureColum = TempColumn(s"$name-structure")
      val fields = sortedChildren.map(f => (currentPath :+ f.name).toCol.isNotNull)
      val arr = array(fields:_*)
      val feat = currentPath.map(p => when(p.toCol.isNotNull, arr)).getOrElse(arr)
      features += feat as structureColum
      aggregates += collect_set_with_count[IndexedSeq[Boolean]].apply(structureColum.col())
      
      val myPath = currentPath
      for (field <- sortedChildren) {
        currentPath = myPath :+ field.name
        field.dataType.accept(this)
      }
    }

    override def visitLeaf(leaf: DataType): Unit = {
      val tempCol = TempColumn(currentPath.get.toDotfreeString)
      features += currentPath.toCol as tempCol
      aggregates += min(tempCol.col())
      aggregates += max(tempCol.col())
      aggregates += approx_count_distinct(tempCol.col())
    }

    override def visitList(list: ArrayType): Unit = {}
    override def visitMap(map: MapType): Unit = {}

    override def result = (features.result(), aggregates.result())
  }

  final case class MakeVersionedStructureVisitor(
    row: Row
  ) extends DataTypeVisitor with SingleResult[VersionTree] {
    assert(row != null)

    val iterator = (0 until row.size).iterator
    var resultStructure: VersionTree = null

    override def visitStruct(schema: StructType): Unit = {
      val sortedFields = schema.fields.sortBy(_.name)
      val names = sortedFields.map(_.name)
      val versions = parseVersions(sortedFields.length)
      val children = for ((field, index) <- sortedFields.zipWithIndex) yield {
        if (versions.forall(!_._1(index))) {
          field.dataType(new SkipNonexistantSubtreeVisitor(iterator))
        } else {
          field.dataType.accept(this)
          resultStructure
        }
      }
      resultStructure = Versions(names, children, versions)
    }

    private def parseVersions(structWidth: Int) = {
      assert(iterator.hasNext)
      val index = iterator.next()
      assert(!row.isNullAt(index))
      val versions = for (version <- row.getSeq[Row](index)) yield {
        val signature = version.getSeq[Boolean](0).toIndexedSeq
        val count = version.getLong(1)
        (signature, count)
      }
      assert(!(versions.map(_._1) contains null))
      if (versions.isEmpty) {
        Seq((IndexedSeq.fill(structWidth)(false), 1.0))
      } else {
        val total = versions.map(_._2).sum.toDouble
        versions.map{case (v, count) => (v, count/total)}
      }
    }

    override def visitLeaf(leaf: DataType): Unit = {
      val minIndex = iterator.next()
      val maxIndex = iterator.next()
      val distinctIndex = iterator.next()
      val metadata = for {
        min <- ColumnValue.fromRow(row, minIndex)
        max <- ColumnValue.fromRow(row, maxIndex)
      } yield {
        val distinct = row.getLong(distinctIndex)
        ColumnMetadata(min, max, distinct)
      }
      resultStructure = Leaf(metadata)
    }

    override def visitList(list: ArrayType): Unit
      = resultStructure = Leaf.empty

    override def visitMap(map: MapType): Unit
      = resultStructure = Leaf.empty

    override def result: VersionTree = resultStructure
  }

  final case class SkipNonexistantSubtreeVisitor(
    metadataToSkip: Iterator[Int]
  ) extends DataTypeVisitor with SingleResult[VersionTree] {
    var resultStructure: VersionTree = null

    override def visitStruct(row: StructType): Unit = {
      metadataToSkip.next()
      val sortedFields = row.fields.sortBy(_.name)
      val names = sortedFields.map(_.name)
      val children = for (field <- sortedFields) yield {
        field.dataType.accept(this)
        resultStructure
      }
      resultStructure = Versions(names, children, Seq((IndexedSeq.fill(sortedFields.size)(false), 1)))
    }

    override def visitLeaf(leaf: DataType): Unit = {
      metadataToSkip.next()
      metadataToSkip.next()
      metadataToSkip.next()
      resultStructure = Leaf.empty
    }

    override def visitList(list: ArrayType): Unit
      = resultStructure = Leaf.empty

    override def visitMap(map: MapType): Unit
      = resultStructure = Leaf.empty

    override def result: VersionTree = resultStructure
  }
}
