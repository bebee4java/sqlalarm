package dt.sql.alarm.output

import dt.sql.alarm.core.{AlarmRecord, Base, Sink}
import org.apache.spark.sql.Dataset
import org.reflections.Reflections

/**
  *
  * Created by songgr on 2019/12/25.
  */
abstract class BaseOutput extends Base {

  def process(data:Dataset[AlarmRecord])

  def fullFormat: String

  def shortFormat: String

}

object SinkInfo {

  import scala.collection.JavaConverters._
  private val outputwithAnnotation = new Reflections(this.getClass.getPackage.getName)
    .getTypesAnnotatedWith(classOf[Sink])

  private val sinkMapping = outputwithAnnotation.asScala.map{ subclass =>
    val name = subclass.getAnnotation(classOf[Sink]).name()
    (name, subclass)
  }.toMap[String, Class[_]]


  def getSink(name:String):BaseOutput = sinkMapping(name).newInstance().asInstanceOf[BaseOutput]

  def getAllSink = sinkMapping.keySet

  def sinkExist(name:String) = sinkMapping.contains(name)

}