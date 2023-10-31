package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.partitions.visitors.PartitionTreeVisitor
import de.unikl.cs.dbis.waves.util.{PartitionFolder, PathKey}

import com.google.gson.{
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

import TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.visitors.SingleResultVisitor
import de.unikl.cs.dbis.waves.util.ColumnValue

/**
  * A TreeNode is any node in the PartitionTree
  */
sealed abstract class TreeNode[+Payload, +Step <: PartitionTreePath] {
    def accept(visitor: PartitionTreeVisitor[Payload]) : Unit
    
    val navigate: PartialFunction[PartitionTreePath, AnyNode[Payload]]

    def apply[T](visitor: SingleResultVisitor[Payload,T]) = {
      accept(visitor)
      visitor.result
    }
}

object  TreeNode {
  val KIND_KEY = "kind"
  type AnyNode[+Payload] = TreeNode[Payload, _ <: PartitionTreePath]
}

/**
  * A Bucket is a Leaf of the PartitionTree which contains actual data.
  * For example, it can contain a string which corresponds to a folder on the
  * DFS which holds the files and is structured like a normal Spark dataset
  *
  * @param name the unique name of the corresponding folder
  */
case class Bucket[+Payload](data: Payload) extends TreeNode[Payload, BucketPath] {
  
  override def accept(visitor: PartitionTreeVisitor[Payload]) = visitor.visit(this)
  
  // override def apply(step: BucketPath): AnyNode[Payload] = ??? // BucketPath has no instances
  override val navigate: PartialFunction[PartitionTreePath,AnyNode[Payload]]
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
case class Spill[+Payload](partitioned: AnyNode[Payload], rest: Bucket[Payload]) extends TreeNode[Payload, SpillPath] {
  override def accept(visitor: PartitionTreeVisitor[Payload]) = visitor.visit(this)

  def apply(step: SpillPath) = step match {
    case Partitioned => partitioned
    case Rest => rest
  }
  
  override val navigate: PartialFunction[PartitionTreePath,AnyNode[Payload]] = {
    case x: SpillPath => apply(x)
  }
}

object Spill {
    val KIND = "spill"
    val REST_KEY = "spill"
    val PARTIOTIONED_KEY = "tree"
}

/**
  * The EvenNWay node is used for partitioning the data into a fixed number of
  * buckets of (approximately) even size.
  * 
  * While the node does not require its children to be buckets, it also
  * provides no means of controlling which data goes into which bucket.
  *
  * @param children
  */
case class EvenNWay[+Payload](
  children: IndexedSeq[AnyNode[Payload]]
) extends TreeNode[Payload, NWayPath] {

  override def accept(visitor: PartitionTreeVisitor[Payload]): Unit
    = visitor.visit(this)

  def apply(step: NWayPath) = children(step.position)

  override val navigate: PartialFunction[PartitionTreePath,AnyNode[Payload]] = {
    case step@NWayPath(index) if children.indices.contains(index) => apply(step)
  }

  def size = children.size
}

object EvenNWay {
  val KIND = "even-nway"
  val CHILDREN_KEY = "children"
}

/**
  * A SplitByPresence node represents a split based on the presence or absence of one node in the document
  *
  * @param key the path to the node whose presence we use to split
  * @param presentKey the subtree containing documents where the key is present
  * @param absentKey the subtree containing documents where the key is absent
  */
case class SplitByPresence[+Payload](
  key: PathKey, presentKey: AnyNode[Payload], absentKey: AnyNode[Payload]
) extends TreeNode[Payload, SplitByPresencePath] {
    override def accept(visitor: PartitionTreeVisitor[Payload]) = visitor.visit(this)

    def apply(step: SplitByPresencePath) = step match {
      case Present => presentKey
      case Absent => absentKey
    }

    override val navigate: PartialFunction[PartitionTreePath, AnyNode[Payload]] = {
      case x: SplitByPresencePath => apply(x)
    }
}

object SplitByPresence {
    val KIND = "present"
    val KEY_KEY = "key"
    val PRESENT_KEY = "present"
    val ABSENT_KEY = "absent"

    def apply[Payload](key: String, presentKey: AnyNode[Payload], absentKey: AnyNode[Payload]) : SplitByPresence[Payload]
        = SplitByPresence(PathKey(key), presentKey, absentKey)

    def apply[Payload](key: PathKey, present: Payload, absent: Payload) : SplitByPresence[Payload]
        = SplitByPresence(key, Bucket(present), Bucket(absent))
    def apply[Payload](key: String, present: Payload, absent: Payload) : SplitByPresence[Payload]
        = apply(key, Bucket(present), Bucket(absent))
}

final case class SplitByValue[+Payload](
  separator: ColumnValue, key: PathKey, less: AnyNode[Payload],more: AnyNode[Payload]
) extends TreeNode[Payload, ValuePath] {

  override def accept(visitor: PartitionTreeVisitor[Payload]): Unit = visitor.visit(this)

  def apply(step: ValuePath) = step match {
    case Less => less
    case MoreOrNull => more
  }

  override val navigate: PartialFunction[PartitionTreePath,AnyNode[Payload]] = {
    case x: ValuePath => apply(x)
  }
}

object SplitByValue {
  val KIND = "value"
  val KEY_KEY = "key"
  val SEPARATOR_KEY = "separator"
  val LESS_KEY = "less"
  val MORE_KEY = "moreOrNull"

  def apply[Payload](separator: ColumnValue, key: String, less: AnyNode[Payload], more: AnyNode[Payload]): SplitByValue[Payload]
    = SplitByValue(separator, PathKey(key), less, more)
  def apply[Payload](separator: ColumnValue, key: PathKey, less: Payload, more: Payload): SplitByValue[Payload]
    = SplitByValue(separator, key, Bucket(less), Bucket(more))
  def apply[Payload](separator: ColumnValue, key: String, less: Payload, more: Payload): SplitByValue[Payload]
    = SplitByValue(separator, PathKey(key), less, more)
}

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

object EvenNWaySerializer extends JsonSerializer[EvenNWay[String]] {
    override def serialize(node: EvenNWay[String], t: Type, ctx: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty(TreeNode.KIND_KEY, EvenNWay.KIND)
        //Constructor with capacity only available starting gson-2.8.0
        val children = new JsonArray(/*node.size*/)
        for (child <- node.children) {
          children.add(ctx.serialize(child))
        }
        obj.add(EvenNWay.CHILDREN_KEY, children)
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

object PartitionByValueSerializer extends JsonSerializer[SplitByValue[String]] {
    override def serialize(node: SplitByValue[String], t: Type, ctx: JsonSerializationContext): JsonElement = {
        val obj = new JsonObject()
        obj.addProperty(TreeNode.KIND_KEY, SplitByValue.KIND)
        obj.addProperty(SplitByValue.KEY_KEY, node.key.toString())
        obj.add(SplitByValue.LESS_KEY, ctx.serialize(node.less))
        obj.add(SplitByValue.MORE_KEY, ctx.serialize(node.more))
        obj.add(SplitByValue.SEPARATOR_KEY, node.separator.toGson)
        obj
    }
}

//
// Deserializers
//

object TreeNodeDeserializer extends JsonDeserializer[AnyNode[String]] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): AnyNode[String] = {
        json match {
          case obj: JsonObject => {
              val kind = obj.get(TreeNode.KIND_KEY).getAsString()
              kind match {
                  case Bucket.KIND => ctx.deserialize[Bucket[String]](obj, classOf[Bucket[String]])
                  case SplitByPresence.KIND => ctx.deserialize[SplitByPresence[String]](obj, classOf[SplitByPresence[String]])
                  case SplitByValue.KIND => ctx.deserialize[SplitByValue[String]](obj, classOf[SplitByValue[String]])
                  case Spill.KIND => ctx.deserialize[Spill[String]](obj, classOf[Spill[String]])
                  case EvenNWay.KIND => ctx.deserialize[EvenNWay[String]](obj, classOf[EvenNWay[String]])
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
          case obj: JsonObject => {
              assert(obj.get(TreeNode.KIND_KEY).getAsString() == Spill.KIND)
              val rest = Bucket(obj.get(Spill.REST_KEY).getAsString())
              val partitioned = ctx.deserialize[AnyNode[String]](obj.get(Spill.PARTIOTIONED_KEY), classOf[AnyNode[String]])
              Spill(partitioned, rest)
          }
          case _ => throw new JsonParseException(s"$json is not an object")
      }
  }
}

object EvenNWayDeserializer extends JsonDeserializer[EvenNWay[String]] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): EvenNWay[String] = {
        json match {
          case obj: JsonObject => {
              assert(obj.get(TreeNode.KIND_KEY).getAsString() == EvenNWay.KIND)
              val children = obj.getAsJsonArray(EvenNWay.CHILDREN_KEY)
              val deserializedChildren = for (index <- 0 until children.size()) yield {
                ctx.deserialize[AnyNode[String]](children.get(index), classOf[AnyNode[String]])
              }
              EvenNWay(deserializedChildren)
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
              val presentKey = ctx.deserialize[AnyNode[String]](obj.get(SplitByPresence.PRESENT_KEY), classOf[AnyNode[String]])
              val absentKey = ctx.deserialize[AnyNode[String]](obj.get(SplitByPresence.ABSENT_KEY), classOf[AnyNode[String]])
              SplitByPresence(key, presentKey, absentKey)
          }
          case _ => throw new JsonParseException(s"$json is not an object")
      }
  }
}

object PartitionByValueDeserializer extends JsonDeserializer[SplitByValue[String]] {
    override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): SplitByValue[String] = {
        json match {
          case obj: JsonObject => {
              assert(obj.get(TreeNode.KIND_KEY).getAsString() == SplitByValue.KIND)
              val key = obj.get(SplitByValue.KEY_KEY).getAsString()
              val lessKey = ctx.deserialize[AnyNode[String]](obj.get(SplitByValue.LESS_KEY), classOf[AnyNode[String]])
              val moreKey = ctx.deserialize[AnyNode[String]](obj.get(SplitByValue.MORE_KEY), classOf[AnyNode[String]])
              val separator = ColumnValue.fromGson(obj.getAsJsonObject(SplitByValue.SEPARATOR_KEY))
                .getOrElse(throw new JsonParseException(s"could not parse separator"))
              SplitByValue(separator, key, lessKey, moreKey)
          }
          case _ => throw new JsonParseException(s"$json is not an object")
      }
  }
}
