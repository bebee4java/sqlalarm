package dt.sql.alarm.conf

import dt.sql.alarm.core.Constants.ALARM_POLICY

object AlarmPolicyConf {

  def getRkey(source:String, topic:String) = List(ALARM_POLICY, source, topic).mkString(":")



/*

{
 "item_id" : "1222",
 ""


}


 */


}
