package de.unikl.cs.dbis.waves.util.operators

import org.apache.spark.sql.types.{DataType, IntegerType, ArrayType}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression, NullIntolerant, ExpectsInputTypes}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.GenericArrayData

final case class AddX(child : Expression, add : Int)
extends UnaryExpression with NullIntolerant with ExpectsInputTypes {

    override def inputTypes = Seq(ArrayType(IntegerType, false))
    override def dataType: DataType = ArrayType(IntegerType, false)
    override def toString(): String = s"add$add($child)"
    
    override protected def nullSafeEval(input: Any): Any = {
        val list = input.asInstanceOf[GenericArrayData].copy()
        for (index <- 0 to list.numElements()-1) {
            list.setInt(index, list.getInt(index)+add)
        }
        list
    }
    
    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
        val index = ctx.freshName("index")
        nullSafeCodeGen(ctx, ev, sd => {
            s"""
                ${ev.value} = $sd.copy();
                for(int $index = 0; $index < ${ev.value}.numElements(); $index++) {
                    ${ev.value}.setInt($index, ${ev.value}.getInt($index)+$add);
                }
            """
        })
    }
    override protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
}
