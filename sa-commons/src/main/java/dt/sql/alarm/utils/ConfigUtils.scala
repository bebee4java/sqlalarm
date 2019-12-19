package dt.sql.alarm.utils

import java.io.FileNotFoundException

import com.typesafe.config.{Config, ConfigFactory}
import dt.sql.alarm.exception.SQLAlarmException


/**
  * 配置工具类
  */
object ConfigUtils {
  var config:Config = _
  try {
    // 默认读取resources目录下的application.conf文件
    config = ConfigFactory.load("application.conf")
  } catch {
    case e:FileNotFoundException => throw new SQLAlarmException("the file application.conf not find in resources path!!")
    case ex:Exception => throw new SQLAlarmException(ex.getMessage, ex)
  }

  def getStringValue(path:String, default: String): String = try {config.getString(path)} catch {
    case ex:Exception => default
  }

  def getStringValue(path:String): String = getStringValue(path, null)


  def getIntValue(path:String, default: Int): Int = try {config.getInt(path)} catch {
    case ex:Exception => default
  }

  def getIntValue(path:String): Int = getIntValue(path, 0)

  def getDoubleValue(path:String, default: Double): Double = try {config.getDouble(path)} catch {
    case ex:Exception => default
  }

  def getDoubleValue(path:String): Double = getDoubleValue(path, 0d)

  def getStringList(path:String):List[String] = try {
    import scala.collection.JavaConverters._
    config.getStringList(path).asScala.toList
  } catch {
    case ex: Exception => List.empty[String]
  }

  def getAnyValue(path:String) = config.getAnyRef(path)

  def parseString(str:String):Config = ConfigFactory.parseString(str)

  def getAnyValueByConfig(config:Config, path:String):AnyRef = {
    if (config != null) {
      config.getAnyRef(path)
    } else {
      null
    }
  }

  def getStringValueByConfig(config:Config, path:String):String = {
    if (config != null) {
      config.getString(path)
    } else {
      null
    }
  }

  def getConfByConfig(config:Config, path:String) = {
    if (config != null) {
      config.getConfig(path)
    } else {
      null
    }
  }
}
