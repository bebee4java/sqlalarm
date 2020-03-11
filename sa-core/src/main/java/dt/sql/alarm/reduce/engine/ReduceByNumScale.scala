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
  * Created by songgr on 2020/03/10.
  */
class ReduceByNumScale(scale: Scale) extends PolicyAnalyzeEngine{

  override def analyse(policy: AlarmPolicyConf, records: Dataset[Row]): Array[EngineResult] = {
    WowLog.logInfo("Noise Reduction Policy: ReduceByNumScale analyzing....")

    val table_rank = records.withColumn(SQL_FIELD_RANK_NAME, row_number()         // rank value
      over( Window.partitionBy(item_id, job_id) orderBy col(event_time).desc ) )
      .withColumn(SQL_FIELD_CURRENT_RECORD_NAME, first(SQL_FIELD_VALUE_NAME)      // current record value
        over( Window.partitionBy(item_id, job_id) orderBy col(event_time).desc ) )
      .withColumn(SQL_FIELD_EARLIEST_RECORD_NAME, last(SQL_FIELD_VALUE_NAME)      // first record value
        over( Window.partitionBy(item_id, job_id) ) )
      .filter(col(SQL_FIELD_RANK_NAME) <= policy.window.value)  // 取出近n条进行分析

    val pendingRecords = table_rank.groupBy(item_id, job_id)
      .agg(
        first(SQL_FIELD_CURRENT_RECORD_NAME).alias(SQL_FIELD_CURRENT_RECORD_NAME), //当前告警记录
        first(SQL_FIELD_EARLIEST_RECORD_NAME).alias(SQL_FIELD_EARLIEST_RECORD_NAME), //历史最早告警记录
        count(alarm).alias(SQL_FIELD_TOTAL_COUNT_NAME), // 总条数
        sum(alarm).alias(SQL_FIELD_ALARM_COUNT_NAME), // 告警条数
        (sum(alarm) / count(alarm)).alias(SQL_FIELD_ALARM_PERCENT_NAME) // 告警记录比例
      )

    val alarmRecords =
      scale match {
        case Number =>
          pendingRecords.filter(col(SQL_FIELD_TOTAL_COUNT_NAME) >= policy.window.value and // 总数必须达到要求条数
            col(SQL_FIELD_ALARM_COUNT_NAME) > policy.policy.value)
        case Percent =>
          pendingRecords.filter(col(SQL_FIELD_TOTAL_COUNT_NAME) >= policy.window.value and // 总数必须达到要求条数
            col(SQL_FIELD_ALARM_PERCENT_NAME) > policy.policy.value)
      }

    val result = alarmRecords.collect().map{
      row =>
        val lastAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_CURRENT_RECORD_NAME), classOf[RecordDetail])
        val firstAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_EARLIEST_RECORD_NAME), classOf[RecordDetail])
        val count = row.getAs[Long](SQL_FIELD_ALARM_COUNT_NAME)
        EngineResult(true, lastAlarmRecord, firstAlarmRecord, count.intValue())
    }

    // 没有产生告警的记录需要入cache
    val cacheDF = table_rank.join(alarmRecords, Seq(item_id,job_id) , "left_outer")
      .filter(isnull(alarmRecords(SQL_FIELD_ALARM_PERCENT_NAME)))
      .select(col(item_id), col(job_id), col(job_stat), col(event_time), col(SQL_FIELD_VALUE_NAME))

    addCache(cacheDF, SaveMode.Overwrite)

    result
  }

}



