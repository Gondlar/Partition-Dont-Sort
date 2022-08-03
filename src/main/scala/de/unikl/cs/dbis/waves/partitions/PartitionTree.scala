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

import de.unikl.cs.dbis.waves.PartitionFolder

/**
  * A partition tree represent a partitionging schema
  *
  * @param globalSchema The data schema
  * @param root the root of the tree of split operations
  */
class PartitionTree(
    val globalSchema: StructType,
    var root: TreeNode = Bucket("spill")
) {
    assert(root != null)
    
    def toJson = PartitionTree.GSON.toJson(this)

    /**
      * Return a Bucket where any data which fits the schema can be inserted
      * if such a location exists. 
      *
      * @return the Bucket, or None if it does not exist
      * @see [[findOrCreateFastInsertLocation]] to create such a location if necessary
      */
    def getFastInsertLocation : Option[Bucket] = root match {
        case bucket@Bucket(_) => Some(bucket)
        case Spill(_,bucket) => Some(bucket)
        case _ => None
    }

    /**
      * Return a Bucket where any data which fits the schema can be inserted.
      * If no such location exists, the tree is changed to provide one.
      *
      * @param nameGenerator a function which provides the name for the newly
      *                      created Bucket if necessary
      * @return the Bucket
      */
    def findOrCreateFastInsertLocation(nameGenerator: () => String) = getFastInsertLocation match {
        case None =>  {
            val rest = Bucket(nameGenerator())
            root = Spill(root, rest)
            rest
        }
        case Some(value) => value
    }

    /**
      * Find all Buckets in the PartitionTree
      *
      * @return an iterator of Buckets
      */
    def getBuckets() = {
        val visitor = new CollectBucketsVisitor()
        root.accept(visitor)
        visitor.iter
    }

    /**
      * Find all Buckets with contents which can satisfy the given filters
      * If no filters are given, this is equivalent to [[getBuckets]]
      *
      * @param filters A collection of filters
      * @return the Buckets
      */
    def getBuckets(filters: Iterable[Filter]) = {
        val visitor = new CollectFilteredBucketsVisitor(filters)
        root.accept(visitor)
        visitor.iter
    }

    /**
      * Get a node represented by navigating along a path.
      * The path consists of String representing the navigational choices
      *
      * @param path the path
      * @return the node at the end of the path or None of no such node exists
      */
    def find(path : Iterable[String]) = {
        val visitor = new FindByPathVisitor(path)
        root.accept(visitor)
        visitor.result
    }

    /**
      * Replace one ocurrence of a subtree with a different subtree.
      * The subtree is matched using object identity, i.e., you need a
      * reference into the tree.
      *
      * @param needle the subtree to be replaced
      * @param replacement the new subtree to be inserted
      */
    def replace(needle: TreeNode, replacement: TreeNode) = {
        val visitor = new ReplaceSubtreeVisitor(needle, replacement)
        root.accept(visitor)
        root = visitor.getResult
    }

    /**
      * Two PartitionTrees re equal if they have the same schema and the same tree structure
      *
      * @param obj the PartitionTree to compare true
      * @return whether the trees are equal
      */
    override def equals(obj: Any): Boolean = obj match {
        case tree : PartitionTree => tree.globalSchema == globalSchema && tree.root == root
        case _ => false
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
        .registerTypeAdapter(classOf[SplitByPresence], PartitionByInnerNodeDeserializer)
        .registerTypeAdapter(classOf[SplitByPresence], PartitionByInnerNodeSerializer)
        .registerTypeAdapter(classOf[TreeNode], TreeNodeDeserializer)
        .registerTypeAdapter(classOf[TreeNode], TreeNodeSerializer)
        .create()
    
    /**
      * Load a PartitionTree from a JSON String
      *
      * @param str the JSON string
      * @return the PartitionTree encoded in the String
      */
    def fromJson(str: String) = GSON.fromJson(str, classOf[PartitionTree])
}

object PartitionTreeSerializer extends JsonSerializer[PartitionTree] {
  override def serialize(tree: PartitionTree, t: Type, ctx: JsonSerializationContext): JsonElement = {
      val obj = new JsonObject
      obj.add(PartitionTree.ROOT_KEY, ctx.serialize(tree.root))
      // do not use schema.toDDL! In general, fromDDL(schema.toDDL) != schema
      obj.addProperty(PartitionTree.SCHEMA_KEY, tree.globalSchema.json) // Yes, this puts a JSON document inside a string in a JSON document. URGH
      obj
  }
}

object PartitionTreeDeserializer extends JsonDeserializer[PartitionTree] {
  override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): PartitionTree = {
      json match {
          case obj: JsonObject => {
              val globalSchema = DataType.fromJson(obj.get(PartitionTree.SCHEMA_KEY).getAsString())
              val root = ctx.deserialize[TreeNode](obj.get(PartitionTree.ROOT_KEY), classOf[TreeNode])
              new PartitionTree(globalSchema.asInstanceOf[StructType], root)
          }
          case _ => throw new JsonParseException(s"$json is not an Object")
      }
  }
}
