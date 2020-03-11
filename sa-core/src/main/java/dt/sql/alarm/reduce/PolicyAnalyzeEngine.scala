package dt.sql.alarm.reduce

import dt.sql.alarm.conf.AlarmPolicyConf
import dt.sql.alarm.core.Constants.SQL_FIELD_VALUE_NAME
import dt.sql.alarm.core.{RecordDetail, RedisOperations, WowLog}
import dt.sql.alarm.core.RecordDetail.{event_time, item_id, job_id, job_stat}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SaveMode}

/**
  * 降噪策略分析引擎
  * Created by songgr on 2020/01/09.
  */
abstract class PolicyAnalyzeEngine {

  def analyse(policy: AlarmPolicyConf, records:Dataset[Row]):Array[EngineResult]

  def addCache(cacheDf: Dataset[Row], mode:SaveMode):Unit = {
    WowLog.logInfo("Add alarm records into redis cache...")
    cacheDf.persist()
    try {
      if (cacheDf.count() > 0) {
        val jobInfos = cacheDf.groupBy(item_id, job_id, job_stat).count().collect().map{
          row =>
            (row.getAs[String](item_id), row.getAs[String](job_id), row.getAs[String](job_stat))
        }
        WowLog.logInfo(s"cache infos:\n ${jobInfos.mkString("\n")}")
        jobInfos.foreach{
          jobInfo =>
            val cache = cacheDf.filter(col(item_id) === jobInfo._1 and col(job_id) === jobInfo._2 and col(job_stat) === jobInfo._3)
              .select(col(SQL_FIELD_VALUE_NAME)).orderBy(col(event_time))
            val key = AlarmPolicyConf.getCacheKey(jobInfo._1, jobInfo._2, jobInfo._3)
            WowLog.logInfo(s"add cache records, key: $key, mode: ${mode.name}")
            RedisOperations.setListCache(key, cache, mode)
        }
      }
    } finally {
      cacheDf.unpersist()
    }
  }

}


case class EngineResult(hasWarning:Boolean,
                        lastAlarmRecord:RecordDetail,
                        firstAlarmRecord:RecordDetail,
                        reduceCount:Int
                       )