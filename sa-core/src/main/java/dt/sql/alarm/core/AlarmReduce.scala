package dt.sql.alarm.core

import dt.sql.alarm.conf.AlarmPolicyConf
import tech.sqlclub.common.log.Logging
import org.apache.spark.sql.Dataset
import dt.sql.alarm.reduce.PolicyAnalyzeEngine
import dt.sql.alarm.reduce.engine.ReduceByTime
import tech.sqlclub.common.utils.JacksonUtils
import org.apache.spark.sql.functions._
import dt.sql.alarm.core.Constants._
import dt.sql.alarm.reduce.EngineResult
import RecordDetail._
import org.apache.spark.sql.expressions.Window
import dt.sql.alarm.conf._
import dt.sql.alarm.conf.PolicyType._
import dt.sql.alarm.conf.WindowType._

/**
  *
  * Created by songgr on 2019/12/25.
  */
object AlarmReduce extends Logging {

  // RecordDetail all fields
  lazy val fields = RecordDetail.getAllFieldName.flatMap(field=> List(lit(field), col(field)) )
  
  def reduce(data:Dataset[RecordDetail], policy: AlarmPolicyConf): Array[EngineResult] = {
    val spark = data.sparkSession
    val engine = getPolicyAnalyzeEngine(policy.policy.`type`, policy.window.`type`)
    // get redis cache
    val redisRdd = RedisOperations.getListCache(AlarmPolicyConf.getCacheKey(policy.item_id) + "*")
    import spark.implicits._
    val cacheRecord = redisRdd.map{
      row =>
      JacksonUtils.fromJson[RecordDetail](row, classOf[RecordDetail])
    }.toDS.withColumn(SQL_FIELD_DATAFROM_NAME, lit(SQL_FIELD_CACHE_NAME))       // add dataFrom col


    val streamRecord = data.withColumn(SQL_FIELD_DATAFROM_NAME, lit(SQL_FIELD_STREAM_NAME))    // add dataFrom col

    val table = streamRecord // stream data union cache data
      .union(cacheRecord)
      .withColumn(SQL_FIELD_VALUE_NAME, to_json(map(fields: _*)))   // add all fields value field

    logInfo("AlarmAlert streamData.union(cacheData) schema: ")
    table.printSchema()

    val result = engine.analyse(policy, table)


    WowLog.logInfo("Policy Engine Analyze result is :")
    logInfo(result.mkString("\n"))

    result
  }

  def cacheReduce(data:Dataset[RecordDetail]): Array[EngineResult] = {
    val spark = data.sparkSession
    val table = data.withColumn(SQL_FIELD_VALUE_NAME, to_json(map(fields: _*)))   // add all fields value field
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
      .withColumn(SQL_FIELD_COUNT_NAME, count(lit(1))                        // record count
        over( Window.partitionBy(item_id, job_id, job_stat) ) )


    val pendingRecords = table.filter(col(SQL_FIELD_RANK_NAME) === 1).
      select(item_id, job_id, job_stat, SQL_FIELD_CURRENT_EVENT_TIME_NAME,SQL_FIELD_CURRENT_RECORD_NAME,
        SQL_FIELD_EARLIEST_EVENT_TIME_NAME,SQL_FIELD_EARLIEST_RECORD_NAME,SQL_FIELD_COUNT_NAME)
      .withColumn(SQL_FIELD_CACHE_DURATION,
        unix_timestamp(col(SQL_FIELD_CURRENT_EVENT_TIME_NAME)) - unix_timestamp(col(SQL_FIELD_EARLIEST_EVENT_TIME_NAME)))
      .withColumn(SQL_FIELD_CACHE_ADD_INTERVAL,
        (unix_timestamp(col(SQL_FIELD_CURRENT_EVENT_TIME_NAME)) - unix_timestamp(col(SQL_FIELD_EARLIEST_EVENT_TIME_NAME)))/col(SQL_FIELD_COUNT_NAME)
      )
      .withColumn(SQL_FIELD_CACHE_UNTIL_TIME,
        unix_timestamp() - unix_timestamp(col(SQL_FIELD_EARLIEST_EVENT_TIME_NAME))
      )

    import spark.implicits._
    pendingRecords.mapPartitions{
      partition =>
        val policies = RedisOperations.getTableCache(ALARM_POLICY + "*")
        val policyMap = policies.map(item => (item._1, AlarmPolicyConf.formJson(item._2))).collect().toMap
        partition.map{
          row =>
            val itemId = row.getAs[String](item_id)
            val jobId = row.getAs[String](job_id)
            val jobStat = row.getAs[String](job_stat)
            val untilTime = row.getAs[Long](SQL_FIELD_CACHE_UNTIL_TIME)
            val cacheAddInterval = row.getAs[Double](SQL_FIELD_CACHE_ADD_INTERVAL)
            val count = row.getAs[Long](SQL_FIELD_COUNT_NAME)
            val key = AlarmPolicyConf.getCacheKey(itemId, jobId, jobStat)
            val policyConf = policyMap.get(itemId)
            if (policyConf.isDefined){
              val policy = policyConf.get
              val windowType = policy.window.`type`.windowType
              val policyType = policy.policy.`type`.policyType
              val overWindow = windowType match {
                case WindowType.time | WindowType.timeCount =>
                  untilTime > policy.window.getTimeWindowSec * 1.2  // 乘1.2 为了和主线岔开, 有几率和主线相交
                case WindowType.number =>
                  untilTime > cacheAddInterval * count * 1.2

              }
              if (overWindow) {
                (policyType, windowType) match {
                  case (PolicyType.scale, _) | (PolicyType.absolute, WindowType.timeCount) =>
                    WowLog.logInfo("the cache has not been merged for a long time, the cache is useless, del it!")
                    RedisOperations.delCache(key)
                    EngineResult(false, null, null, -1)
                  case (PolicyType.absolute, WindowType.time) | (PolicyType.absolute, WindowType.number) =>
                    if (count == 1 && policy.policy.alertFirst ) {
                      WowLog.logInfo("this alarm record has been pushed, del it!")
                      RedisOperations.delCache(key)
                      EngineResult(false, null, null, -1)
                    } else {
                      WowLog.logInfo(s"the record cache has warning and merged by daemon clean server. Agg count: $count, key: $key.")
                      val lastAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_CURRENT_RECORD_NAME), classOf[RecordDetail])
                      val firstAlarmRecord = JacksonUtils.fromJson(row.getAs[String](SQL_FIELD_EARLIEST_RECORD_NAME), classOf[RecordDetail])
                      EngineResult(true, lastAlarmRecord, firstAlarmRecord, count.intValue())
                    }
                }
              } else {
                WowLog.logInfo(s"the record cache is under window, ignore it! key: $key.")
                // 没超过窗口 不聚合告警
                EngineResult(false, null, null, -1)
              }
            } else {
              // 没有匹配的聚合策略 删除key
              logWarning(s"has no policy, ignore it! del the key: $key.")
              RedisOperations.delCache(key)
              EngineResult(false, null, null, -1)
            }
        }

    }.collect()
  }

  def getPolicyAnalyzeEngine(policyType:String, windowType:String):PolicyAnalyzeEngine = {
    (policyType.policyType, windowType.windowType) match {
      case (PolicyType.absolute, WindowType.time) => ReduceByTime
    }
  }

}
