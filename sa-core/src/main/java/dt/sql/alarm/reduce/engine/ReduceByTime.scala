package dt.sql.alarm.reduce.engine

import dt.sql.alarm.conf.AlarmPolicyConf
import dt.sql.alarm.core.{RecordDetail, WowLog}
import dt.sql.alarm.reduce.{EngineResult, PolicyAnalyzeEngine}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import dt.sql.alarm.core.Constants._
import tech.sqlclub.common.utils.JacksonUtils

/**
  *
  * Created by songgr on 2020/01/09.
  */
object ReduceByTime extends PolicyAnalyzeEngine {

  override def analyse(policy: AlarmPolicyConf, records: Dataset[Row]): Array[EngineResult] = {
    WowLog.logInfo("Noise Reduction Policy: ReduceByTime analyzing....")

    val fields = RecordDetail.getAllFieldName.flatMap(field=> List(lit(field), col(field)) )
    val table = records.withColumn(SQL_FIELD_VALUE_NAME, to_json(map(fields: _*)))

    // group by job_id,job_stat order by event_time desc
    val table_rank = table
      .withColumn(SQL_FIELD_CURRENT_RECORD_NAME, first(SQL_FIELD_VALUE_NAME)
        over( Window.partitionBy(RecordDetail.job_id, RecordDetail.job_stat) orderBy col(RecordDetail.event_time).desc ) )
      .withColumn(SQL_FIELD_EARLIEST_RECORD_NAME, last(SQL_FIELD_VALUE_NAME)
        over( Window.partitionBy(RecordDetail.job_id, RecordDetail.job_stat) ) )
      .withColumn(SQL_FIELD_CURRENT_EVENT_TIME_NAME, first(RecordDetail.event_time)
        over( Window.partitionBy(RecordDetail.job_id, RecordDetail.job_stat) orderBy col(RecordDetail.event_time).desc ) )
      .withColumn(SQL_FIELD_EARLIEST_EVENT_TIME_NAME, last(RecordDetail.event_time)
        over( Window.partitionBy(RecordDetail.job_id, RecordDetail.job_stat) ) )
      .withColumn(SQL_FIELD_RANK_NAME, row_number()
        over( Window.partitionBy(RecordDetail.job_id, RecordDetail.job_stat) orderBy col(RecordDetail.event_time).desc ) )
      .withColumn(SQL_FIELD_DATAFROM_NAME, min(SQL_FIELD_DATAFROM_NAME)
        over( Window.partitionBy(RecordDetail.job_id, RecordDetail.job_stat) ) )
      .withColumn(SQL_FIELD_COUNT_NAME, count(lit(1))
        over( Window.partitionBy(RecordDetail.job_id, RecordDetail.job_stat) ) )

    val pendingRecords = table_rank.filter(col(SQL_FIELD_RANK_NAME) === 1).
      select(SQL_FIELD_CURRENT_EVENT_TIME_NAME,SQL_FIELD_CURRENT_RECORD_NAME,
        SQL_FIELD_EARLIEST_EVENT_TIME_NAME,SQL_FIELD_EARLIEST_RECORD_NAME,SQL_FIELD_DATAFROM_NAME,SQL_FIELD_COUNT_NAME)

    val firstAlarmRecords = if (policy.policy.alertFirst) {
      val firstAlarmRecords = pendingRecords.filter(
        col(SQL_FIELD_DATAFROM_NAME) === SQL_FIELD_STREAM_NAME and  // only from stream
        col(SQL_FIELD_COUNT_NAME) === 1  // and count=1
      )

      firstAlarmRecords.collect().map {
        row=>
          val firstAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_CURRENT_RECORD_NAME), classOf[RecordDetail])
          EngineResult(true, firstAlarmRecord, firstAlarmRecord, 1)
      }

    } else {
      Array(EngineResult(false, null, null, -1))
    }

    val alarmRecords = pendingRecords.filter(
      unix_timestamp(col(SQL_FIELD_CURRENT_EVENT_TIME_NAME)) -
        unix_timestamp(col(SQL_FIELD_EARLIEST_EVENT_TIME_NAME)) >= policy.window.getTimeWindowSec
    )

    val streamAlarmRecords = alarmRecords.collect().map{
      row =>
        val lastAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_CURRENT_RECORD_NAME), classOf[RecordDetail])
        val firstAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_EARLIEST_RECORD_NAME), classOf[RecordDetail])
        val count = row.getAs[Long](SQL_FIELD_COUNT_NAME)
        EngineResult(true, lastAlarmRecord, firstAlarmRecord, count.intValue())
    }

    WowLog.logInfo("Noise Reduction Policy: ReduceByTime analysis completed!!")

    firstAlarmRecords ++ streamAlarmRecords
  }
}
