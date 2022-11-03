package de.unikl.cs.dbis.waves.util

package object functional {
  implicit class UnixPipe[F](val value: F) {

    /**
      * An operator similar to the unix pipe. The value on its left hand side is
      * passed into the function on its right hand side and its result is
      * returned. This allows chaining functions with a nicer synatx.
      * 
      * E.g., `f(g(h(v)))` is the same as `v |> h |> g |> f`
      *
      * @param f
      * @return
      */
    def |>[G] (f: F => G) = f(value)
  }
}
