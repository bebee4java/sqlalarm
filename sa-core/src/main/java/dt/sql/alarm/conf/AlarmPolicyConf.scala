package dt.sql.alarm.conf


import dt.sql.alarm.core.Constants.ALARM_POLICY
import dt.sql.alarm.core.Constants.ALARM_CACHE
import tech.sqlclub.common.utils.JacksonUtils

case class AlarmPolicyConf(item_id:String, window:Window, policy:Policy)
case class Window(`type`: String, value:Int, unit:String, count:Int){
  def getTimeWindowSec = {
    import Unit._
    val u = unit.unit match {
      case Unit.m => 60
      case Unit.h => 3600
      case Unit.d => 86400
    }
    value * u
  }

}
case class Policy(`type`:String, agg:String, value: Double, first_alert:Int){
  def alertFirst = 1 == first_alert
}


object WindowType extends Enumeration{
  implicit class WindowTypeString(s:String){
    def windowType:Value = WindowType.withName(s)
    def isTime:Boolean = time == windowType
    def isNumber:Boolean = number == windowType
    def isTimeCount:Boolean = timeCount == windowType
  }
  type Type = Value
  val time,number,timeCount = Value
}

object Unit extends Enumeration{
  implicit class UnitString(s:String){
    def unit:Value = Unit.withName(s)
  }
  type Type = Value
  val m,h,d,n = Value
}

object PolicyType extends Enumeration{
  implicit class PolicyTypeString(s:String){
    def policyType:Value = PolicyType.withName(s)
    def isAbsolute:Boolean = absolute == policyType
    def isScale:Boolean = scale == policyType
  }
  type Type = Value
  val absolute,scale = Value
}

object Agg extends Enumeration{
  type Type = Value
  implicit class AggString(s:String){
    def agg:Value = Agg.withName(s)
    def isPercent:Boolean = percent == agg
  }
  val count,percent = Value
}


object AlarmPolicyConf {

  def getRkey(source:String, topic:String) = List(ALARM_POLICY, source, topic).mkString(":")

  def getCacheKey(itemId:String) = List(ALARM_CACHE,itemId).mkString(":")

  def formJson(json:String) = JacksonUtils.fromJson[AlarmPolicyConf](json, classOf[AlarmPolicyConf])

  def prettyString(policyConf: AlarmPolicyConf): String = JacksonUtils.prettyPrint(policyConf)


  def main(args: Array[String]): Unit = {

    val s =
      """
        |{
        | "item_id" : "1222",
        | "window": {
        |   "type": "time",
        |   "value": 10,
        |   "unit": "m"
        | },
        | "policy":{
        |   "type":"absolute"
        | }
        |}
      """.stripMargin

    println(s)

    val policy = JacksonUtils.fromJson(s, classOf[AlarmPolicyConf])

    println(policy.window.`type`)

    println(policy)
  }

/*

{
    "item_id":"1222",
    "window":{
        "type":"time/number/timeCount",
        "value":10,
        "unit":"m/h/d/n",
        "count":0
    },
    "policy":{
        "type":"absolute/scale",
        "agg":"count/percent",
        "value":0.9/100,
        "first_alert": 0
    }
}

 */
}
