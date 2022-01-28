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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.sources.Filter

import de.unikl.cs.dbis.waves.PartitionFolder

class PartitionTree(
    val globalSchema: StructType,
    var root: TreeNode = Bucket("spill")
) {
    assert(root != null)
    
    def toJson = PartitionTree.GSON.toJson(this)

    def fastInsertLocation : Option[Bucket] = root match {
        case bucket@Bucket(_) => Some(bucket)
        case Spill(_,bucket) => Some(bucket)
        case _ => None
    }

    def createSpillPartition(getter: () => String) = root match {
        case bucket@Bucket(_) => bucket
        case Spill(_, rest) => rest
        case other => {
            val rest = Bucket(getter())
            root = Spill(root, rest)
            rest
        }
    }

    def getBuckets() = {
        val visitor = new CollectBucketsVisitor()
        root.accept(visitor)
        visitor.iter
    }

    def getBuckets(filters: Array[Filter]) = {
        val visitor = new CollectFilteredBucketsVisitor(filters)
        root.accept(visitor)
        visitor.iter
    }

    def replace(needle: TreeNode, replacement: TreeNode) = {
        val visitor = new ReplaceSubtreeVisitor(needle, replacement)
        root.accept(visitor)
        root = visitor.getResult
    }
}

object PartitionTree {
    val ROOT_KEY = "root"
    val SCHEMA_KEY = "schema"

    private val GSON = new GsonBuilder()
        .registerTypeAdapter(classOf[PartitionTree], PartitionTreeDeserializer)
        .registerTypeAdapter(classOf[PartitionTree], PartitionTreeSerializer)
        .registerTypeAdapter(classOf[Bucket], BucketDeserializer)
        .registerTypeAdapter(classOf[Bucket], BucketSerializer)
        .registerTypeAdapter(classOf[Spill], SpillDeserializer)
        .registerTypeAdapter(classOf[Spill], SpillSerializer)
        .registerTypeAdapter(classOf[PartitionByInnerNode], PartitionByInnerNodeDeserializer)
        .registerTypeAdapter(classOf[PartitionByInnerNode], PartitionByInnerNodeSerializer)
        .registerTypeAdapter(classOf[TreeNode], TreeNodeDeserializer)
        .registerTypeAdapter(classOf[TreeNode], TreeNodeSerializer)
        .create()
    
    def fromJson(str: String) = GSON.fromJson(str, classOf[PartitionTree])
}

object PartitionTreeSerializer extends JsonSerializer[PartitionTree] {
  override def serialize(tree: PartitionTree, t: Type, ctx: JsonSerializationContext): JsonElement = {
      val obj = new JsonObject
      obj.add(PartitionTree.ROOT_KEY, ctx.serialize(tree.root))
      obj.addProperty(PartitionTree.SCHEMA_KEY, tree.globalSchema.toDDL)
      obj
  }
}

object PartitionTreeDeserializer extends JsonDeserializer[PartitionTree] {
  override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): PartitionTree = {
      json match {
          case obj: JsonObject => {
              val globalSchema = StructType.fromDDL(obj.get(PartitionTree.SCHEMA_KEY).getAsString())
              val root = ctx.deserialize[TreeNode](obj.get(PartitionTree.ROOT_KEY), classOf[TreeNode])
              new PartitionTree(globalSchema, root)
          }
          case _ => throw new JsonParseException(s"$json is not an Object")
      }
  }
}
