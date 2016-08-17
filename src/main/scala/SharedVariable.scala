import scala.reflect.ClassTag

class SharedVariable[T: ClassTag](constructor: => T) extends AnyRef with Serializable{
  @transient private lazy val instance: T = constructor
  def get = instance
}

object SharedVariable{
  def apply[T: ClassTag](constructor: => T):SharedVariable[T] = new SharedVariable[T](constructor)
}