package dt.sql.alarm.input


import dt.sql.alarm.core.{Base, Source}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.reflections.Reflections


abstract class BaseInput extends Base {

  def getDataSetStream(spark:SparkSession):Dataset[Row]

  def fullFormat: String

  def shortFormat: String

}


object SourceInfo {

  import scala.collection.JavaConverters._
  private val inputWithAnnotation = new Reflections(this.getClass.getPackage.getName)
    .getTypesAnnotatedWith(classOf[Source])

  private val sourceMapping = inputWithAnnotation.asScala.map{subclass =>
    val name = subclass.getAnnotation(classOf[Source]).name()
    (name, subclass)
  }.toMap[String, Class[_]]


  def getSource(name:String):BaseInput = sourceMapping(name).newInstance().asInstanceOf[BaseInput]

  def getAllSource = sourceMapping.keySet

  def sourceExist(name:String) = sourceMapping.contains(name)

}

