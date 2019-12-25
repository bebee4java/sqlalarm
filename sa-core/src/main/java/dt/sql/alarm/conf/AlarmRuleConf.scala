package dt.sql.alarm.conf

import dt.sql.alarm.core.Constants.ALARM_RULE
import dt.sql.alarm.utils.JacksonUtil

case class AlarmRuleConf(platform:String, source:Source, filter:Filter)
case class Source(`type`:String, topic:String)
case class Filter(table:String, structure:Array[Field], sql:String)
case class Field(name:String, `type`:String, xpath:String)

object AlarmRuleConf {
  def getRkey(source:String, topic:String) = List(ALARM_RULE, source, topic).mkString(":")

  def toJson(ruleConf: AlarmRuleConf) = JacksonUtil.toJson(ruleConf)

  def formJson(json:String) = JacksonUtil.fromJson[AlarmRuleConf](json, classOf[AlarmRuleConf])

}



