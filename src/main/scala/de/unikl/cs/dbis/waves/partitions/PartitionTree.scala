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

class PartitionTree(
    val globalSchema: StructType,
    var tree: TreeNode = null,
    val spill: Bucket = Bucket(PartitionTree.SPILL_KEY)
) {
    def spillKey = spill.name
    def hasTree = tree != null
    def accept(visitor: PartitionTreeVisitor) = visitor.visit(this)
    def toJson = PartitionTree.GSON.toJson(this)
}

object PartitionTree {
    val SPILL_KEY = "spill"
    val ROOT_KEY = "root"
    val SCHEMA_KEY = "schema"

    private val GSON = new GsonBuilder()
        .registerTypeAdapter(PartitionTree.getClass(), PartitionTreeDeserializer)
        .registerTypeAdapter(PartitionTree.getClass(), PartitionTreeSerializer)
        .registerTypeAdapter(Bucket.getClass(), BucketDeserializer)
        .registerTypeAdapter(Bucket.getClass(), BucketSerializer)
        .registerTypeAdapter(PartitionByInnerNode.getClass(), PartitionByInnerNodeDeserializer)
        .registerTypeAdapter(PartitionByInnerNode.getClass(), PartitionByInnerNodeSerializer)
        .registerTypeAdapter(TreeNode.getClass(), TreeNodeDeserializer)
        .registerTypeAdapter(TreeNode.getClass(), TreeNodeSerializer)
        .create()
    
    def fromJson(str: String) = GSON.fromJson(str, PartitionTree.getClass())
}

object PartitionTreeSerializer extends JsonSerializer[PartitionTree] {
  override def serialize(tree: PartitionTree, t: Type, ctx: JsonSerializationContext): JsonElement = {
      val obj = new JsonObject
      obj.add(PartitionTree.SPILL_KEY, ctx.serialize(tree.spill))
      if (tree.hasTree) {
          obj.add(PartitionTree.ROOT_KEY, ctx.serialize(tree.tree))
      }
      obj.addProperty(PartitionTree.SCHEMA_KEY, tree.globalSchema.toDDL)
      obj
  }
}

object PartitionTreeDeserializer extends JsonDeserializer[PartitionTree] {
  override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): PartitionTree = {
      json match {
          case obj: JsonObject => {
              val spill = ctx.deserialize[Bucket](obj.get(PartitionTree.SPILL_KEY), Bucket.getClass())
              val globalSchema = StructType.fromDDL(obj.get(PartitionTree.SCHEMA_KEY).getAsString())
              val root = if (obj.has(PartitionTree.ROOT_KEY)) {
                  ctx.deserialize[TreeNode](obj.get(PartitionTree.ROOT_KEY), TreeNode.getClass())
              } else null
              new PartitionTree(globalSchema, root, spill)
          }
          case _ => throw new JsonParseException(s"$json is not an Object")
      }
  }
}
