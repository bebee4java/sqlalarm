package dt.sql.alarm.core

import dt.sql.alarm.conf.AlarmPolicyConf
import dt.sql.alarm.reduce.EngineResult
import tech.sqlclub.common.log.Logging

object AlarmAlert extends Logging {

  def push(results:Array[EngineResult]) : Unit = {
    results.filter(_.hasWarning).foreach {
      result =>
        logInfo(result.toString)
        val recordDetail = result.lastAlarmRecord
        val firstEventTime = result.firstAlarmRecord.event_time
        val count = result.reduceCount
        WowLog.logInfo(s"this moment the record has warning! Agg count: $count")
        if ( send(AlarmRecord.as(recordDetail), firstEventTime, count) && count >1 ) {
          val key = AlarmPolicyConf.getCacheKey(recordDetail.item_id, recordDetail.job_id, recordDetail.job_stat)
          RedisOperations.delCache(key)
          WowLog.logInfo(s"agg over, del the cache! key: $key")
        }
    }

  }

  def send(alarmRecord: AlarmRecord, firstTime:String, count:Int):Boolean = {
    logInfo("Alarm record call send api...")
    true
  }

  case class AlarmRecord(
      job_id:String,
      job_stat:String,
      event_time:String,
      message:String,
      context:String,       // map string
      title:String,
      platform:String,
      item_id:String
  )

  object AlarmRecord {
    def as(recordDetail: RecordDetail) = AlarmRecord(
      recordDetail.job_id,
      recordDetail.job_stat,
      recordDetail.event_time,
      recordDetail.message,
      recordDetail.context,
      recordDetail.title,
      recordDetail.platform,
      recordDetail.item_id
    )
  }

}
