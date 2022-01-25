package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.PartitionFolder
import de.unikl.cs.dbis.waves.PathKey

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

sealed abstract class TreeNode {
    def accept(visitor: PartitionTreeVisitor) : Unit
}

object  TreeNode {
    val KIND_KEY = "kind"
}

case class Bucket(name: String) extends TreeNode {
    def folder(basePath : String) = new PartitionFolder(basePath, name, false)

    override def accept(visitor: PartitionTreeVisitor) = visitor.visit(this)
}

object Bucket {
    val KIND = "bucket"
    val NAME_KEY = "name"
}

case class PartitionByInnerNode(key: PathKey, presentKey: TreeNode, absentKey: TreeNode) extends TreeNode {
    override def accept(visitor: PartitionTreeVisitor) = visitor.visit(this)
}

object PartitionByInnerNode {
    val KIND = "present"
    val KEY_KEY = "key"
    val PRESENT_KEY = "present"
    val ABSENT_KEY = "absent"

    def apply(key: String, presentKey: TreeNode, absentKey: TreeNode) : PartitionByInnerNode
        = PartitionByInnerNode(PathKey(key), presentKey, absentKey)
}

//TODO PartitionByCondition

//
// Serializers
//

object BucketSerializer extends JsonSerializer[Bucket] {
    override def serialize(bucket: Bucket, t: Type, ctx: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty(TreeNode.KIND_KEY, Bucket.KIND)
        obj.addProperty(Bucket.NAME_KEY, bucket.name)
        obj
    }
}

object PartitionByInnerNodeSerializer extends JsonSerializer[PartitionByInnerNode] {
    override def serialize(node: PartitionByInnerNode, t: Type, ctx: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty(TreeNode.KIND_KEY, PartitionByInnerNode.KIND)
        obj.addProperty(PartitionByInnerNode.KEY_KEY, node.key.toString())
        obj.add(PartitionByInnerNode.PRESENT_KEY, ctx.serialize(node.presentKey))
        obj.add(PartitionByInnerNode.ABSENT_KEY, ctx.serialize(node.absentKey))
        obj
    }
}

object TreeNodeSerializer extends JsonSerializer[TreeNode] {
  override def serialize(node: TreeNode, t: Type, ctx: JsonSerializationContext): JsonElement = {
      node match {
          case bucket@Bucket(_) => ctx.serialize(bucket)
          case node@PartitionByInnerNode(_, _, _) => ctx.serialize(node)
      }
  }
}

//
// Deserializers
//

object TreeNodeDeserializer extends JsonDeserializer[TreeNode] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): TreeNode = {
        json match {
          case obj: JsonObject => {
              val kind = obj.get(TreeNode.KIND_KEY).getAsString()
              kind match {
                  case Bucket.KIND => ctx.deserialize[Bucket](obj, Bucket.getClass())
                  case PartitionByInnerNode.KIND => ctx.deserialize[PartitionByInnerNode](obj, PartitionByInnerNode.getClass())
                  case unknown => throw new JsonParseException(s"kind \"$unknown\" is unknown")
              }
          }
          case _ => throw new JsonParseException(s"$json is not an object")
      }
  }
}

object BucketDeserializer extends JsonDeserializer[Bucket] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): Bucket = {
        json match {
          case obj: JsonObject => {
              assert(obj.get(TreeNode.KIND_KEY).getAsString() == Bucket.KIND)
              val name = obj.get(Bucket.NAME_KEY).getAsString()
              Bucket(name)
          }
          case _ => throw new JsonParseException(s"$json is not an object")
      }
  }
}

object PartitionByInnerNodeDeserializer extends JsonDeserializer[PartitionByInnerNode] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): PartitionByInnerNode = {
        json match {
          case obj: JsonObject => {
              assert(obj.get(TreeNode.KIND_KEY).getAsString() == PartitionByInnerNode.KIND)
              val key = obj.get(PartitionByInnerNode.KEY_KEY).getAsString()
              val presentKey = ctx.deserialize[TreeNode](obj.get(PartitionByInnerNode.PRESENT_KEY), TreeNode.getClass())
              val absentKey = ctx.deserialize[TreeNode](obj.get(PartitionByInnerNode.ABSENT_KEY), TreeNode.getClass())
              PartitionByInnerNode(key, presentKey, absentKey)
          }
          case _ => throw new JsonParseException(s"$json is not an object")
      }
  }
}
