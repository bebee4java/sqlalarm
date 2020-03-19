package dt.sql.alarm.reduce.engine

import dt.sql.alarm.conf.AlarmPolicyConf
import dt.sql.alarm.core.Constants._
import dt.sql.alarm.core.RecordDetail._
import dt.sql.alarm.core.{RecordDetail, WowLog}
import dt.sql.alarm.reduce.{EngineResult, PolicyAnalyzeEngine}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import tech.sqlclub.common.utils.JacksonUtils

/**
  *
  * Created by songgr on 2020/03/11.
  */
class ReduceByTimeScale(scale: Scale) extends PolicyAnalyzeEngine{

  override def analyse(policy: AlarmPolicyConf, records: Dataset[Row]): Array[EngineResult] = {
    WowLog.logInfo("Noise Reduction Policy: ReduceByTimeScale analyzing....")

    val table = records
      .withColumn(SQL_FIELD_CURRENT_EVENT_TIME_NAME, first(event_time)   // current event time
        over( Window.partitionBy(item_id, job_id) orderBy col(event_time).desc ) )
      // 取出近T时间进行分析
      .filter(unix_timestamp(col(SQL_FIELD_CURRENT_EVENT_TIME_NAME)) -
        unix_timestamp(col(event_time)) <= policy.window.getTimeWindowSec
      )

    table.persist()

    try {
      val alarmEndpoints = table
        .filter(col(alarm) === 1)
        .withColumn(SQL_FIELD_CURRENT_RECORD_NAME, first(SQL_FIELD_VALUE_NAME)      // current record value
          over( Window.partitionBy(item_id, job_id) orderBy col(event_time).desc ) )
        .withColumn(SQL_FIELD_EARLIEST_RECORD_NAME, last(SQL_FIELD_VALUE_NAME)      // first record value
          over( Window.partitionBy(item_id, job_id) ) )
        .groupBy(item_id, job_id)
        .agg(
          first(SQL_FIELD_CURRENT_RECORD_NAME).alias(SQL_FIELD_CURRENT_RECORD_NAME), //当前告警记录
          first(SQL_FIELD_EARLIEST_RECORD_NAME).alias(SQL_FIELD_EARLIEST_RECORD_NAME) //历史最早告警记录
        )


      val pendingRecords = table.groupBy(item_id, job_id)
        .agg(
          (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))).alias(SQL_FIELD_EVENT_TIME_DURATION_NAME), //时间距离差
          count(alarm).alias(SQL_FIELD_TOTAL_COUNT_NAME), // 总条数
          sum(alarm).alias(SQL_FIELD_ALARM_COUNT_NAME), // 告警条数
          (sum(alarm) / count(alarm)).alias(SQL_FIELD_ALARM_PERCENT_NAME) // 告警记录比例
        )


      val alarmRecords =
        scale match {
          case Number =>
            pendingRecords.filter(col(SQL_FIELD_ALARM_COUNT_NAME) > policy.policy.getValue) // 告警条数达到要求
          case Percent =>
            pendingRecords.filter(col(SQL_FIELD_EVENT_TIME_DURATION_NAME) >= policy.window.value and // 时长间隔达到窗口
              col(SQL_FIELD_ALARM_PERCENT_NAME) > policy.policy.getValue)
        }

      val result = alarmRecords.join(alarmEndpoints, Seq(item_id,job_id), "left_outer").collect().map{
        row =>
          val lastAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_CURRENT_RECORD_NAME), classOf[RecordDetail])
          val firstAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_EARLIEST_RECORD_NAME), classOf[RecordDetail])
          val count = row.getAs[Long](SQL_FIELD_ALARM_COUNT_NAME)
          EngineResult(true, lastAlarmRecord, firstAlarmRecord, count.intValue())
      }

      // 没有产生告警的记录需要入cache
      val cacheDF = table.join(alarmRecords, Seq(item_id,job_id) , "left_outer")
        .filter(isnull(alarmRecords(SQL_FIELD_ALARM_PERCENT_NAME)))
        .select(col(item_id), col(job_id), col(job_stat), col(event_time), col(SQL_FIELD_VALUE_NAME))

      addCache(cacheDF, SaveMode.Overwrite)

      result

    } finally {
      table.unpersist()
    }

  }

}



