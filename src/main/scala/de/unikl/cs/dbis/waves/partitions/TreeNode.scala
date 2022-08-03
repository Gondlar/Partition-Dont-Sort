package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.PartitionFolder
import de.unikl.cs.dbis.waves.util.PathKey

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

/**
  * A TreeNode is any node in the PartitionTree
  */
sealed abstract class TreeNode {
    def accept(visitor: PartitionTreeVisitor) : Unit
}

object  TreeNode {
    val KIND_KEY = "kind"
}

/**
  * A Bucket is a Leaf of the PartitionTree which contains actual data.
  * It corresponds to a folder on the DFS which contains the files and is
  * structured like a normal Spark dataset
  *
  * @param name the unique name of the corresponding folder
  */
case class Bucket(name: String) extends TreeNode {
    def folder(basePath : String) = new PartitionFolder(basePath, name, false)

    override def accept(visitor: PartitionTreeVisitor) = visitor.visit(this)
}

object Bucket {
    val KIND = "bucket"
    val NAME_KEY = "name"
}

/**
  * A Spill node is a place to fit documents which do not fit the partitioned schema
  *
  * @param partitioned The remainder of the PartitionTree
  * @param rest A Bucket for all nodes that do not conform to the further partition schema
  */
case class Spill(partitioned: TreeNode, rest: Bucket) extends TreeNode {
    override def accept(visitor: PartitionTreeVisitor) = visitor.visit(this)
}

object Spill {
    val KIND = "spill"
    val REST_KEY = "spill"
    val PARTIOTIONED_KEY = "tree"
}

/**
  * A SplitByPresence node represents a split based on the presence or absence of one node in the document
  *
  * @param key the path to the node whose presence we use to split
  * @param presentKey the subtree containing documents where the key is present
  * @param absentKey the subtree containing documents where the key is absent
  */
case class SplitByPresence(key: PathKey, presentKey: TreeNode, absentKey: TreeNode) extends TreeNode {
    override def accept(visitor: PartitionTreeVisitor) = visitor.visit(this)
}

object SplitByPresence {
    val KIND = "present"
    val KEY_KEY = "key"
    val PRESENT_KEY = "present"
    val ABSENT_KEY = "absent"

    def apply(key: String, presentKey: TreeNode, absentKey: TreeNode) : SplitByPresence
        = SplitByPresence(PathKey(key), presentKey, absentKey)

    def apply(key: String, present: String, absent: String) : SplitByPresence
        = apply(key, Bucket(present), Bucket(absent))
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

object SpillSerializer extends JsonSerializer[Spill] {
    override def serialize(node: Spill, t: Type, ctx: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty(TreeNode.KIND_KEY, Spill.KIND)
        obj.add(Spill.REST_KEY, ctx.serialize(node.rest.name))
        obj.add(Spill.PARTIOTIONED_KEY, ctx.serialize(node.partitioned))
        obj
    }
}

object PartitionByInnerNodeSerializer extends JsonSerializer[SplitByPresence] {
    override def serialize(node: SplitByPresence, t: Type, ctx: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty(TreeNode.KIND_KEY, SplitByPresence.KIND)
        obj.addProperty(SplitByPresence.KEY_KEY, node.key.toString())
        obj.add(SplitByPresence.PRESENT_KEY, ctx.serialize(node.presentKey))
        obj.add(SplitByPresence.ABSENT_KEY, ctx.serialize(node.absentKey))
        obj
    }
}

object TreeNodeSerializer extends JsonSerializer[TreeNode] {
  override def serialize(node: TreeNode, t: Type, ctx: JsonSerializationContext): JsonElement = {
      node match {
          case bucket@Bucket(_) => ctx.serialize(bucket)
          case node@SplitByPresence(_, _, _) => ctx.serialize(node)
          case spill@Spill(_,_) => ctx.serialize(spill)
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
                  case Bucket.KIND => ctx.deserialize[Bucket](obj, classOf[Bucket])
                  case SplitByPresence.KIND => ctx.deserialize[SplitByPresence](obj, classOf[SplitByPresence])
                  case Spill.KIND => ctx.deserialize[Spill](obj, classOf[Spill])
                  case unknown => throw new JsonParseException(s"kind '$unknown' is unknown")
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

object SpillDeserializer extends JsonDeserializer[Spill] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): Spill = {
        json match {
          case obj: JsonObject => {//TODO hier aufgehört
              assert(obj.get(TreeNode.KIND_KEY).getAsString() == Spill.KIND)
              val rest = Bucket(obj.get(Spill.REST_KEY).getAsString())
              val partitioned = ctx.deserialize[TreeNode](obj.get(Spill.PARTIOTIONED_KEY), classOf[TreeNode])
              Spill(partitioned, rest)
          }
          case _ => throw new JsonParseException(s"$json is not an object")
      }
  }
}

object PartitionByInnerNodeDeserializer extends JsonDeserializer[SplitByPresence] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): SplitByPresence = {
        json match {
          case obj: JsonObject => {
              assert(obj.get(TreeNode.KIND_KEY).getAsString() == SplitByPresence.KIND)
              val key = obj.get(SplitByPresence.KEY_KEY).getAsString()
              val presentKey = ctx.deserialize[TreeNode](obj.get(SplitByPresence.PRESENT_KEY), classOf[TreeNode])
              val absentKey = ctx.deserialize[TreeNode](obj.get(SplitByPresence.ABSENT_KEY), classOf[TreeNode])
              SplitByPresence(key, presentKey, absentKey)
          }
          case _ => throw new JsonParseException(s"$json is not an object")
      }
  }
}
