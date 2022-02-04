package de.unikl.cs.dbis.waves.util

class ValByNeed[T](
    private val init: () => T
) {
    var contents : Option[T] = None

    def map(f : T => T) = contents = Some(contents match {
        case Some(value) => f(value)
        case None => f(init())
    })

    def modify(f : T => Unit) = contents match {
        case Some(value) => f(value)
        case None => {
            val v = init()
            f(v)
            contents = Some(v)
        }
    }

    def optionalMap(f : T => T) = contents = contents match {
        case Some(value) => Some(f(value))
        case None => None
    }

    def optionalModify(f : T => Unit) = contents match {
        case Some(value) => f(value)
        case None => ()
    }
}
