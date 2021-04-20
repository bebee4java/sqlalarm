package dt.sql.alarm.reduce.engine

import dt.sql.alarm.conf.AlarmPolicyConf
import dt.sql.alarm.core.{RecordDetail, WowLog}
import dt.sql.alarm.reduce.{EngineResult, PolicyAnalyzeEngine}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.functions._
import dt.sql.alarm.core.Constants._
import tech.sqlclub.common.utils.JacksonUtils
import dt.sql.alarm.core.RecordDetail._

/**
  *
  * Created by songgr on 2020/01/09.
  */
class ReduceByWindow(window: AggWindow) extends PolicyAnalyzeEngine {

  override def analyse(policy: AlarmPolicyConf, records: Dataset[Row]):Array[EngineResult] = {
    WowLog.logInfo("Noise Reduction Policy: ReduceByWindow analyzing....")

    // filter alarm records
    val table = records.filter(col(alarm) === 1)

    // group by job_id,job_stat order by event_time desc
    val table_rank = table
      .withColumn(SQL_FIELD_CURRENT_RECORD_NAME, first(SQL_FIELD_VALUE_NAME)      // current record value
        over( Window.partitionBy(item_id, job_id, job_stat) orderBy col(event_time).desc ) )
      .withColumn(SQL_FIELD_EARLIEST_RECORD_NAME, last(SQL_FIELD_VALUE_NAME)      // first record value
        over( Window.partitionBy(item_id, job_id, job_stat) ) )
      .withColumn(SQL_FIELD_CURRENT_EVENT_TIME_NAME, first(event_time)   // current event time
        over( Window.partitionBy(item_id, job_id, job_stat) orderBy col(event_time).desc ) )
      .withColumn(SQL_FIELD_EARLIEST_EVENT_TIME_NAME, last(event_time)   // first event time
        over( Window.partitionBy(item_id, job_id, job_stat) ) )
      .withColumn(SQL_FIELD_RANK_NAME, row_number()                                  // rank value
        over( Window.partitionBy(item_id, job_id, job_stat) orderBy col(event_time).desc ) )
      .withColumn(SQL_FIELD_DATAFROM_NAME, min(SQL_FIELD_DATAFROM_NAME)              // datafrom value is cache if has record which from redis cache
        over( Window.partitionBy(item_id, job_id, job_stat) ) )
      .withColumn(SQL_FIELD_COUNT_NAME, count(lit(1))                        // record count
        over( Window.partitionBy(item_id, job_id, job_stat) ) )

    val pendingRecords = table_rank.filter(col(SQL_FIELD_RANK_NAME) === 1).
      select(item_id, job_id, job_stat, SQL_FIELD_CURRENT_EVENT_TIME_NAME,SQL_FIELD_CURRENT_RECORD_NAME,
        SQL_FIELD_EARLIEST_EVENT_TIME_NAME,SQL_FIELD_EARLIEST_RECORD_NAME,SQL_FIELD_DATAFROM_NAME,SQL_FIELD_COUNT_NAME)

    pendingRecords.persist()

    try {
      // first alarm
      val firstAlarmRecords = if (policy.policy.alertFirst) {
        val firstAlarmRecords = pendingRecords.filter(
          col(SQL_FIELD_DATAFROM_NAME) === SQL_FIELD_STREAM_NAME and  // only from stream
            col(SQL_FIELD_COUNT_NAME) >= 1  // and count>=1
        )

        firstAlarmRecords.collect().map {
          row=>
            val firstAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_CURRENT_RECORD_NAME), classOf[RecordDetail])
            EngineResult(true, firstAlarmRecord, firstAlarmRecord, 1)
        }

      } else {
        Array(EngineResult(false, null, null, -1))
      }

      val alarmRecords = window match {
        case NumberWindow =>
          pendingRecords.filter(col(SQL_FIELD_COUNT_NAME) >= policy.window.value )
        case TimeWindow =>
          pendingRecords.filter(
            unix_timestamp(col(SQL_FIELD_CURRENT_EVENT_TIME_NAME)) -
              unix_timestamp(col(SQL_FIELD_EARLIEST_EVENT_TIME_NAME)) >= policy.window.getTimeWindowSec
          )
        // 近T时间达到n条
        case TimeCountWindow =>
          pendingRecords.filter(
            unix_timestamp(col(SQL_FIELD_CURRENT_EVENT_TIME_NAME)) -
              unix_timestamp(col(SQL_FIELD_EARLIEST_EVENT_TIME_NAME)) <= policy.window.getTimeWindowSec
            and
              col(SQL_FIELD_COUNT_NAME) >= policy.window.count
          )
      }

      val streamAlarmRecords = alarmRecords.collect().map{
        row =>
          val lastAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_CURRENT_RECORD_NAME), classOf[RecordDetail])
          val firstAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_EARLIEST_RECORD_NAME), classOf[RecordDetail])
          val count = row.getAs[Long](SQL_FIELD_COUNT_NAME)
          EngineResult(true, lastAlarmRecord, firstAlarmRecord, count.intValue())
      }

      WowLog.logInfo(s"Noise Reduction Policy: ReduceByWindow analysis completed! windowType:${policy.window.`type`}, alarm records size:${streamAlarmRecords.length}")

      // 没有产生告警的记录需要入cache
      val cacheDF = table.join(alarmRecords, Seq(item_id,job_id,job_stat) , "left_outer")
        .filter(isnull(alarmRecords(SQL_FIELD_CURRENT_EVENT_TIME_NAME)) and table(SQL_FIELD_DATAFROM_NAME) === SQL_FIELD_STREAM_NAME) // 只加流记录
        .select(col(item_id), col(job_id), col(job_stat), col(event_time), col(SQL_FIELD_VALUE_NAME))

      addCache(cacheDF, SaveMode.Append)

      firstAlarmRecords ++ streamAlarmRecords
    } finally {
      pendingRecords.unpersist()
    }
  }
}


