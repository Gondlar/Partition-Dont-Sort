class SchemaAssumption(
  val path : String,
  val present : Boolean
){}

object SchemaAssumptionFactory {
  def present(path: String) = new SchemaAssumption(path, true)
  def absent(path: String) = new SchemaAssumption(path, false)
}

class SchemaAssumptions (
    val assumptions : SchemaAssumption*
) {
    def present(path: String) : Boolean = {
        for (assumption <- assumptions) {
            if (assumption.present && assumption.path.startsWith(path))
                return true
            if (!assumption.present && path.startsWith(assumption.path))
                return false
        }
        return true;
    }
}