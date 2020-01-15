package dt.sql.alarm.core

import dt.sql.alarm.conf.AlarmPolicyConf
import tech.sqlclub.common.log.Logging
import org.apache.spark.sql.Dataset
import dt.sql.alarm.conf
import dt.sql.alarm.reduce.PolicyAnalyzeEngine
import dt.sql.alarm.reduce.engine.ReduceByTime
import tech.sqlclub.common.utils.JacksonUtils
import org.apache.spark.sql.functions.lit
import dt.sql.alarm.core.Constants._



/**
  *
  * Created by songgr on 2019/12/25.
  */
object AlarmAlert extends Logging {

  def push(itemId:String, source: conf.Source, data:Dataset[AlarmRecord]): Unit = {
    val spark = data.sparkSession
    WowLog.logInfo(s"alarm alert start with item: $itemId, source:${source.`type`}, topic:${source.topic} .....")
    val policyConf = RedisOperations.getTableCache(AlarmPolicyConf.getRkey(source.`type`, source.topic), itemId)

    val policy = AlarmPolicyConf.formJson(policyConf)
    val engine = getPolicyAnalyzeEngine(policy.policy.`type`, policy.window.`type`)

    // get redis cache
    val redisRdd = RedisOperations.getListCache(AlarmPolicyConf.getCacheKey(itemId) + "*")
    import spark.implicits._

    val cacheAlarmRecord = redisRdd.map{
      row =>
      JacksonUtils.fromJson[AlarmRecord](row, classOf[AlarmRecord])
    }.toDS.withColumn(SQL_FIELD_DATAFROM_NAME, lit(SQL_FIELD_CACHE_NAME))       // add dataFrom col


    val streamAlarmRecord = data.withColumn(SQL_FIELD_DATAFROM_NAME, lit(SQL_FIELD_STREAM_NAME))    // add dataFrom col

    val table = streamAlarmRecord // stream data union cache data
      .union(cacheAlarmRecord)

    logInfo("AlarmAlert streamData.union(cacheData) schema: ")
    table.printSchema()

    val result = engine.analyse(policy, table)

    WowLog.logInfo("Policy Engine Analyze result is :")
    logInfo(result.mkString("\n"))

    if (result.nonEmpty) WowLog.logInfo("===== Alert! =====")
  }


  def getPolicyAnalyzeEngine(policyType:String, windowType:String):PolicyAnalyzeEngine = {
    import dt.sql.alarm.conf._
    import dt.sql.alarm.conf.PolicyType._
    import dt.sql.alarm.conf.WindowType._

    (policyType.policyType, windowType.windowType) match {
      case (PolicyType.absolute, WindowType.time) => ReduceByTime
    }
  }

}
