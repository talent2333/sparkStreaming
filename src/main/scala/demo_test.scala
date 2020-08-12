import scala.util.Random

/**
 * Author talent2333
 * Date 2020/7/22 11:13
 * Description 
 */
object demo_test {
  def main(args: Array[String]): Unit = {

    val person = new Person

    //（1）判断对象是否为某个类型的实例
    val bool: Boolean = person.isInstanceOf[Person]

    if ( bool ) {
      //（2）将对象转换为某个类型的实例
      val p1: Person = person.asInstanceOf[Person]
      println(p1)
    }

    //（3）获取类的信息
    val pClass: Class[Person] = classOf[Person]
    println(pClass.getName)
  }

}
class Person{

}
object Person {

}
