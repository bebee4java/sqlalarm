package dt.sql.alarm.utils

import com.typesafe.config.{Config, ConfigFactory}
import dt.sql.alarm.exception.SQLAlarmException
import dt.sql.alarm.log.Logging
import scala.collection.JavaConversions._

/**
  * 配置工具类
  */
object ConfigUtils extends Logging {
  var config:Config = _
  try {
    val url = this.getClass.getClassLoader.getResource("application.conf")
    if (url != null) {
      // 默认读取resources目录下的application.conf文件
      config = ConfigFactory.parseURL(url)
    } else {
      logWarning("Didn't find the config file application.conf in classpath!")
      config = ConfigFactory.empty()
    }
  } catch {
    case ex:Exception => throw new SQLAlarmException(ex.getMessage, ex)
  }

  def configBuilder(map : Map[String, Object]) = {
    val tempMap = (map.keySet -- getKeys).map(k => (k, map.get(k).get)).toMap
    val unionMap = toMap ++ tempMap
    config = ConfigFactory.parseMap(unionMap)
  }
  
  def getConfigKeys(config: Config):Set[String] = {
    config.entrySet()
      .map(map => map.getKey).toSet
  }

  def getKeys = getConfigKeys(config)

  def config2Map(config:Config):Map[String, AnyRef] = {
    getConfigKeys(config)
      .map(k => (k , config.getAnyRef(k))).toMap
  }

  def toMap = config2Map(config)

  def config2StringMap(config: Config):Map[String, String] = {
    getConfigKeys(config)
      .map(k => (k, config.getString(k))).toMap
  }

  def toStringMap = config2StringMap(config)

  def hasConfig(path:String):Boolean = config.hasPath(path)

  def getConfig(path:String):Config = try {config.getConfig(path)} catch {
    case ex:Exception => null
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
