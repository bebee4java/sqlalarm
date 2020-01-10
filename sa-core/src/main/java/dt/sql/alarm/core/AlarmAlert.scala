package dt.sql.alarm.core

import dt.sql.alarm.conf.AlarmPolicyConf
import tech.sqlclub.common.log.Logging
import org.apache.spark.sql.Dataset
import dt.sql.alarm.conf

/**
  *
  * Created by songgr on 2019/12/25.
  */
object AlarmAlert extends Logging {

 def push(itemId:String, source: conf.Source, data:Dataset[AlarmRecord]): Unit = {
   WowLog.logInfo(s"alarm alert start with item: $itemId, source:${source.`type`}, topic:${source.topic} .....")
   val policyConf = RedisOperations.getTableCache(AlarmPolicyConf.getRkey(source.`type`, source.topic), itemId)

 }

}
