package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.util.{PartitionFolder, PathKey}

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
sealed abstract class TreeNode[Payload] {
    type PathType <: PartitionTreePath

    def accept(visitor: PartitionTreeVisitor[Payload]) : Unit
    def apply(step: PartitionTreePath): TreeNode[Payload] = navigate.applyOrElse(step, { _: PartitionTreePath =>
      throw InvalidPathException(Seq(step), step, "step does not fit node type")
    })
    
    val navigate: PartialFunction[PartitionTreePath, TreeNode[Payload]]
}

object  TreeNode {
    val KIND_KEY = "kind"
}

/**
  * A Bucket is a Leaf of the PartitionTree which contains actual data.
  * For example, it can contain a string which corresponds to a folder on the
  * DFS which holds the files and is structured like a normal Spark dataset
  *
  * @param name the unique name of the corresponding folder
  */
case class Bucket[Payload](data: Payload) extends TreeNode[Payload] {

  
  override type PathType = BucketPath
  
  override def accept(visitor: PartitionTreeVisitor[Payload]) = visitor.visit(this)
  
  override val navigate: PartialFunction[PartitionTreePath,TreeNode[Payload]]
    = PartialFunction.empty
}

object Bucket {
    val KIND = "bucket"
    val NAME_KEY = "name"

    /**
      * Create a new Bucket with a random name according to the PartitionFolder name chooser
      */
    def apply() : Bucket[String] = Bucket(PartitionFolder.makeFolder("").name)

    /**
      * Convenience methods for Buckets containing path information
      */
    implicit class PathBucket(bucket: Bucket[String]) {
        def folder(basePath : String) = new PartitionFolder(basePath, bucket.data, false)
    }
}

/**
  * A Spill node is a place to fit documents which do not fit the partitioned schema
  *
  * @param partitioned The remainder of the PartitionTree
  * @param rest A Bucket for all nodes that do not conform to the further partition schema
  */
case class Spill[Payload](partitioned: TreeNode[Payload], rest: Bucket[Payload]) extends TreeNode[Payload] {

  
  override type PathType = SpillPath
  
  override def accept(visitor: PartitionTreeVisitor[Payload]) = visitor.visit(this)
  
  override val navigate: PartialFunction[PartitionTreePath,TreeNode[Payload]] = {
    case Partitioned => partitioned
    case Rest => rest
  }
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
case class SplitByPresence[Payload](key: PathKey, presentKey: TreeNode[Payload], absentKey: TreeNode[Payload]) extends TreeNode[Payload] {
    override type PathType = SplitByPresencePath

    override def accept(visitor: PartitionTreeVisitor[Payload]) = visitor.visit(this)

    override val navigate: PartialFunction[PartitionTreePath,TreeNode[Payload]] = {
      case Present => presentKey
      case Absent => absentKey
    }
}

object SplitByPresence {
    val KIND = "present"
    val KEY_KEY = "key"
    val PRESENT_KEY = "present"
    val ABSENT_KEY = "absent"

    def apply[Payload](key: String, presentKey: TreeNode[Payload], absentKey: TreeNode[Payload]) : SplitByPresence[Payload]
        = SplitByPresence(PathKey(key), presentKey, absentKey)

    def apply(key: String, present: String, absent: String) : SplitByPresence[String]
        = apply(key, Bucket(present), Bucket(absent))
}

//TODO PartitionByCondition

//
// Serializers
//

object BucketSerializer extends JsonSerializer[Bucket[String]] {
    override def serialize(bucket: Bucket[String], t: Type, ctx: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty(TreeNode.KIND_KEY, Bucket.KIND)
        obj.addProperty(Bucket.NAME_KEY, bucket.data)
        obj
    }
}

object SpillSerializer extends JsonSerializer[Spill[String]] {
    override def serialize(node: Spill[String], t: Type, ctx: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty(TreeNode.KIND_KEY, Spill.KIND)
        obj.add(Spill.REST_KEY, ctx.serialize(node.rest.data))
        obj.add(Spill.PARTIOTIONED_KEY, ctx.serialize(node.partitioned))
        obj
    }
}

object PartitionByInnerNodeSerializer extends JsonSerializer[SplitByPresence[String]] {
    override def serialize(node: SplitByPresence[String], t: Type, ctx: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty(TreeNode.KIND_KEY, SplitByPresence.KIND)
        obj.addProperty(SplitByPresence.KEY_KEY, node.key.toString())
        obj.add(SplitByPresence.PRESENT_KEY, ctx.serialize(node.presentKey))
        obj.add(SplitByPresence.ABSENT_KEY, ctx.serialize(node.absentKey))
        obj
    }
}

//
// Deserializers
//

object TreeNodeDeserializer extends JsonDeserializer[TreeNode[String]] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): TreeNode[String] = {
        json match {
          case obj: JsonObject => {
              val kind = obj.get(TreeNode.KIND_KEY).getAsString()
              kind match {
                  case Bucket.KIND => ctx.deserialize[Bucket[String]](obj, classOf[Bucket[String]])
                  case SplitByPresence.KIND => ctx.deserialize[SplitByPresence[String]](obj, classOf[SplitByPresence[String]])
                  case Spill.KIND => ctx.deserialize[Spill[String]](obj, classOf[Spill[String]])
                  case unknown => throw new JsonParseException(s"kind '$unknown' is unknown")
              }
          }
          case _ => throw new JsonParseException(s"$json is not an object")
      }
  }
}

object BucketDeserializer extends JsonDeserializer[Bucket[String]] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): Bucket[String] = {
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

object SpillDeserializer extends JsonDeserializer[Spill[String]] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): Spill[String] = {
        json match {
          case obj: JsonObject => {//TODO hier aufgehört
              assert(obj.get(TreeNode.KIND_KEY).getAsString() == Spill.KIND)
              val rest = Bucket(obj.get(Spill.REST_KEY).getAsString())
              val partitioned = ctx.deserialize[TreeNode[String]](obj.get(Spill.PARTIOTIONED_KEY), classOf[TreeNode[String]])
              Spill(partitioned, rest)
          }
          case _ => throw new JsonParseException(s"$json is not an object")
      }
  }
}

object PartitionByInnerNodeDeserializer extends JsonDeserializer[SplitByPresence[String]] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): SplitByPresence[String] = {
        json match {
          case obj: JsonObject => {
              assert(obj.get(TreeNode.KIND_KEY).getAsString() == SplitByPresence.KIND)
              val key = obj.get(SplitByPresence.KEY_KEY).getAsString()
              val presentKey = ctx.deserialize[TreeNode[String]](obj.get(SplitByPresence.PRESENT_KEY), classOf[TreeNode[String]])
              val absentKey = ctx.deserialize[TreeNode[String]](obj.get(SplitByPresence.ABSENT_KEY), classOf[TreeNode[String]])
              SplitByPresence(key, presentKey, absentKey)
          }
          case _ => throw new JsonParseException(s"$json is not an object")
      }
  }
}
