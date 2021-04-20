package dt.sql.alarm.conf


import dt.sql.alarm.core.Constants.ALARM_POLICY
import dt.sql.alarm.core.Constants.ALARM_CACHE
import tech.sqlclub.common.utils.JacksonUtils

case class AlarmPolicyConf(item_id:String, window:Window, policy:Policy)
case class Window(`type`: String, value:Int, unit:String, count:Int){
  def getTimeWindowSec = {
    import WindowUnit._
    val u = unit.unit match {
      case WindowUnit.m => 60
      case WindowUnit.h => 3600
      case WindowUnit.d => 86400
    }
    value * u
  }

}
case class Policy(`type`:String, unit:String, value: Double, first_alert:Int){
  def alertFirst = 1 == first_alert

  import PolicyUnit._
  def getValue = if (unit.isPercent) norm(value / 100.0d) else value

  def norm(d:Double) = {
    d match {
      case x if x>=1 => 1.0d
      case x if x<=0 => 0.0d
      case _ => d
    }
  }
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

object WindowUnit extends Enumeration{
  implicit class WindowUnitString(s:String){
    def unit:Value = WindowUnit.withName(s)
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

object PolicyUnit extends Enumeration{
  type Type = Value
  implicit class PolicyUnitString(s:String){
    def unit:Value = PolicyUnit.withName(s)
    def isPercent:Boolean = percent == unit
  }
  val number,percent = Value
}


object AlarmPolicyConf {

  def getRkey(source:String, topic:String) = List(ALARM_POLICY, source, topic).mkString(":")

  def getCacheKey(itemId:String) = List(ALARM_CACHE,itemId).mkString(":")

  def getCacheKey(itemId:String, jobId:String) = List(ALARM_CACHE,itemId,jobId).mkString(":")

  def getCacheKey(itemId:String, jobId:String, jobStat:String) = List(ALARM_CACHE, itemId, jobId, jobStat).mkString(":")

  def formJson(json:String) = JacksonUtils.fromJson[AlarmPolicyConf](json, classOf[AlarmPolicyConf])

  def prettyString(policyConf: AlarmPolicyConf): String = JacksonUtils.prettyPrint(policyConf)


  def main(args: Array[String]): Unit = {

    val d = Policy("", "number", 30, 1).getValue

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
        "unit":"number/percent",
        "value":0.9/100,
        "first_alert": 0
    }
}

 */
}
