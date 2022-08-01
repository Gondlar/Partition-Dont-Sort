package de.unikl.cs.dbis.waves

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.parquet.ShouldNeverHappenException

import de.unikl.cs.dbis.waves.util.PathKey

/**
  * All transformations maintain the order of keys throughout the schema tree
  */
object SchemaTransforms {

  /**
    * Modifies one field of the schema using the given transformer
    *
    * @param key the string identifier of the field to be modified
    * @param schema the StructType to be modified
    * @param f the transformer to be applied to schema(key)
    * @return a copy of schema with schema(key) replaced by f(schema(key))
    */
  private def modifyOne(key: String, schema: StructType, f: StructType => StructType)
    = StructType(schema.fields.map(field => {
        if (field.name == key) {
            field.copy(dataType = f(field.dataType.asInstanceOf[StructType]))
        } else field
    }))

  /**
    * Modifies a field deep within a schema
    *
    * @param key the path to the field to be modified
    * @param schema the StructTpe to be modified
    * @param f the transformer to be applied. Takes the final key and the final StructType on the path as an argument
    * @return a copy of schema with the given replacement applied
    */
  private def modifyDeep(key: PathKey, schema: StructType, f: (String, StructType) => StructType) : StructType
    = if (key.isNested) {
      modifyOne(key.head, schema, partial => modifyDeep(key.tail, partial, f))
    } else f(key.head, schema)

  /**
    * Removes the given schema entry
    *
    * @param key path to the entry or subtree to be removed
    * @param schema schema to be modified
    * @return a copy of schema where the entry for key is always missing
    */
  def alwaysAbsent(key: PathKey, schema: StructType)
    = modifyDeep(key, schema, (subkey, partial) => {
        StructType(partial.fields.filter(field => field.name != subkey))
    })

  /**
    * marks the given schema entry as not nullable
    *
    * @param key path to the entry or subtree to be removed
    * @param schema schema to be modified
    * @return a copy of the schema where the entry for key is not nullable
    */
  def alwaysPresent(key: PathKey, schema: StructType)
    = modifyDeep(key, schema, (subkey, partial) => {
        StructType(partial.fields.map(field => if (field.name == subkey) field.copy(nullable = false) else field))
    })

}
