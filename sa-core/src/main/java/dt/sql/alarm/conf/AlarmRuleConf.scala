package dt.sql.alarm.conf

import dt.sql.alarm.core.Constants.ALARM_RULE
import dt.sql.alarm.utils.JacksonUtil

case class AlarmRuleConf(item_id:String, platform:String, title:String, source:Source, filter:Filter)
case class Source(`type`:String, topic:String)
case class Filter(table:String, structure:Array[Field], sql:String)
case class Field(name:String, `type`:String, xpath:String)

object AlarmRuleConf {
  def getRkey(source:String, topic:String) = List(ALARM_RULE, source, topic).mkString(":")

  def toJson(ruleConf: AlarmRuleConf) = JacksonUtil.toJson(ruleConf)

  def formJson(json:String) = JacksonUtil.fromJson[AlarmRuleConf](json, classOf[AlarmRuleConf])

  def prettyString(ruleConf: AlarmRuleConf): String = JacksonUtil.prettyPrint(ruleConf)


  def main(args: Array[String]): Unit = {
    println(prettyString(AlarmRuleConf("1222","alarm","sql alarm",
      Source("kafka", " sqlalarm_event"),
        Filter("error_job",
          Array(Field("job_id","string","$.jobid")),
          "select jobid from sqlalarm_event"
        )
      )
    ))

  }

  /*
  {
    "item_id" : "1222",
    "platform" : "alarm",
    "title" : "sql alarm",
    "source" : {
      "type" : "kafka",
      "topic" : " sqlalarm_event"
    },
    "filter" : {
      "table" : "error_job",
      "structure" : [ {
      "name" : "job_id",
      "type" : "string",
      "xpath" : "$.jobid"
    } ],
      "sql" : "select jobid from sqlalarm_event"
    }
  }
  */
}



