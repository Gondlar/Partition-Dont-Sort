package de.unikl.cs.dbis.waves.partitions

import org.apache.hadoop.shaded.com.google.gson.{
    JsonDeserializer,
    JsonSerializer,
    JsonElement,
    JsonObject,
    JsonArray,
    JsonDeserializationContext,
    JsonSerializationContext,
    JsonParseException,
    GsonBuilder
}
import java.lang.reflect.Type
import org.apache.spark.sql.types.{StructType, DataType}
import org.apache.spark.sql.sources.Filter

import de.unikl.cs.dbis.waves.partitions.visitors._
import de.unikl.cs.dbis.waves.util.PartitionFolder
import org.apache.spark.sql.catalyst.InternalRow

import TreeNode.AnyNode

/**
  * A partition tree represent a partitionging schema
  *
  * @param globalSchema The data schema
  * @param root the root of the tree of split operations
  */
class PartitionTree[Payload](
    val globalSchema: StructType,
    var root: AnyNode[Payload] = Bucket("spill")
) {
    assert(root != null)

    /**
      * Return a Bucket where any data which fits the schema can be inserted
      * if such a location exists. 
      *
      * @return the Bucket, or None if it does not exist
      * @see [[findOrCreateFastInsertLocation]] to create such a location if necessary
      */
    def getFastInsertLocation : Option[Bucket[Payload]] = root match {
        case bucket@Bucket(_) => Some(bucket)
        case Spill(_,bucket) => Some(bucket)
        case _ => None
    }

    /**
      * Return a Bucket where any data which fits the schema can be inserted.
      * If no such location exists, the tree is changed to provide one.
      *
      * @param payloadGenerator a function which provides the name for the newly
      *                         created Bucket if necessary
      * @return the Bucket
      */
    def findOrCreateFastInsertLocation(payloadGenerator: () => Payload) = getFastInsertLocation match {
        case None =>  {
            val rest = Bucket(payloadGenerator())
            root = Spill(root, rest)
            rest
        }
        case Some(value) => value
    }

    /**
      * Two PartitionTrees re equal if they have the same schema and the same tree structure
      *
      * @param obj the PartitionTree to compare true
      * @return whether the trees are equal
      */
    override def equals(obj: Any): Boolean = obj match {
        case tree : PartitionTree[Payload] => tree.globalSchema == globalSchema && tree.root == root
        case _ => false
    }
}

object PartitionTree {
    val ROOT_KEY = "root"
    val SCHEMA_KEY = "schema"

    private val GSON = new GsonBuilder()
        .registerTypeAdapter(classOf[PartitionTree[String]], PartitionTreeDeserializer)
        .registerTypeAdapter(classOf[PartitionTree[String]], PartitionTreeSerializer)
        .registerTypeAdapter(classOf[Bucket[String]], BucketDeserializer)
        .registerTypeAdapter(classOf[Bucket[String]], BucketSerializer)
        .registerTypeAdapter(classOf[Spill[String]], SpillDeserializer)
        .registerTypeAdapter(classOf[Spill[String]], SpillSerializer)
        .registerTypeAdapter(classOf[SplitByPresence[String]], PartitionByInnerNodeDeserializer)
        .registerTypeAdapter(classOf[SplitByPresence[String]], PartitionByInnerNodeSerializer)
        .registerTypeAdapter(classOf[AnyNode[String]], TreeNodeDeserializer)
        .create()

    implicit class PathPartitionTree(tree: PartitionTree[String]) {
        def toJson = PartitionTree.GSON.toJson(tree)
    }
    
    /**
      * Load a PartitionTree from a JSON String
      *
      * @param str the JSON string
      * @return the PartitionTree encoded in the String
      */
    def fromJson(str: String) = GSON.fromJson(str, classOf[PartitionTree[String]])
}

object PartitionTreeSerializer extends JsonSerializer[PartitionTree[String]] {
  override def serialize(tree: PartitionTree[String], t: Type, ctx: JsonSerializationContext): JsonElement = {
      val obj = new JsonObject
      obj.add(PartitionTree.ROOT_KEY, ctx.serialize(tree.root))
      // do not use schema.toDDL! In general, fromDDL(schema.toDDL) != schema
      obj.addProperty(PartitionTree.SCHEMA_KEY, tree.globalSchema.json) // Yes, this puts a JSON document inside a string in a JSON document. URGH
      obj
  }
}

object PartitionTreeDeserializer extends JsonDeserializer[PartitionTree[String]] {
  override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): PartitionTree[String] = {
      json match {
          case obj: JsonObject => {
              val globalSchema = DataType.fromJson(obj.get(PartitionTree.SCHEMA_KEY).getAsString())
              val root = ctx.deserialize[AnyNode[String]](obj.get(PartitionTree.ROOT_KEY), classOf[AnyNode[String]])
              new PartitionTree(globalSchema.asInstanceOf[StructType], root)
          }
          case _ => throw new JsonParseException(s"$json is not an Object")
      }
  }
}
