package de.unikl.cs.dbis.waves.util

class ValByNeed[T](
    private val init: () => T
) {
    var contents : Option[T] = None

    def map(f : T => T) = contents = Some(contents match {
        case Some(value) => f(value)
        case None => f(init())
    })

    def modify[R](f : T => R) : R = contents match {
        case Some(value) => f(value)
        case None => {
            val v = init()
            val res = f(v)
            contents = Some(v)
            res
        }
    }

    def optionalMap(f : T => T) = contents = contents match {
        case Some(value) => Some(f(value))
        case None => None
    }

    def optionalModify[R](f : T => R) : Option[R] = contents match {
        case Some(value) => Some(f(value))
        case None => None
    }
}
