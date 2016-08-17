import java.util.UUID

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

/**
 * Created by root on 6/30/16.
 */
class SharedSingleton [T: ClassTag](constructor: => T) extends  AnyRef with Serializable{

  val singletonUUID = UUID.randomUUID().toString

  @transient private lazy val instance: T={
    SharedSingleton.singletonPool.synchronized{
      val singletonOption = SharedSingleton.singletonPool.get(singletonUUID)
      if (singletonOption.isEmpty){
        SharedSingleton.singletonPool.put(singletonUUID, constructor)
      }
    }
    SharedSingleton.singletonPool.get(singletonUUID).get.asInstanceOf[T]
  }

  def get = instance
}

object SharedSingleton{
  private val singletonPool  =  new TrieMap[String, Any]()

  def apply[T: ClassTag](constructor: => T):SharedSingleton[T] = new SharedSingleton[T](constructor)
  def poolSize: Int = singletonPool.size

  def poolClear(): Unit = singletonPool.clear()
}
