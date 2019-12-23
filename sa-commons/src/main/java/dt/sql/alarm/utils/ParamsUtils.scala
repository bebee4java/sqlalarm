package dt.sql.alarm.utils

import java.util.{HashMap, Map => JMap}

import scala.Array._

class ParamsUtils {

  private lazy val params:JMap[String,String] = new HashMap[String,String]()

  def this(args: Array[String]) {
    this()
    // (arg, index)
    val argsWithIndex = args.zip(range(0, args.length))
    argsWithIndex.filter(tuple => tuple._1.startsWith("-")).foreach{
    tuple =>
      params.put(tuple._1.substring(1), args(tuple._2 + 1))
    }
  }

  def hasParam(key: String): Boolean = params.containsKey(key)

  def getParam(key: String) = params.get(key)

  def getParam(key: String, defaultValue: String): String = {
    val value = params.get(key)
    if (value == null || "" == value) defaultValue else value
  }

  import scala.collection.JavaConverters._
  def getParamsMap: Map[String, String] = params.asScala.toMap




  def getIntParam(key: String, defaultValue: Int): Int = NumberUtils.getIntValue(getParam(key), defaultValue)

  def getIntParam(key: String) : Int = getParam(key).toInt

  def getBooleanParam(key: String): Boolean = "true" == getParam(key, "").toLowerCase

  def getBooleanParam(key: String, defaultValue: Boolean): Boolean = if (hasParam(key)) getBooleanParam(key) else defaultValue
}
