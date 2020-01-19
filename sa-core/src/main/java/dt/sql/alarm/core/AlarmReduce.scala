package dt.sql.alarm.core

import dt.sql.alarm.conf.AlarmPolicyConf
import tech.sqlclub.common.log.Logging
import org.apache.spark.sql.Dataset
import dt.sql.alarm.reduce.PolicyAnalyzeEngine
import dt.sql.alarm.reduce.engine.ReduceByTime
import tech.sqlclub.common.utils.JacksonUtils
import org.apache.spark.sql.functions.lit
import dt.sql.alarm.core.Constants._
import dt.sql.alarm.reduce.EngineResult

/**
  *
  * Created by songgr on 2019/12/25.
  */
object AlarmReduce extends Logging {

  def reduce(policy: AlarmPolicyConf, data:Dataset[RecordDetail]): Array[EngineResult] = {
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

    logInfo("AlarmAlert streamData.union(cacheData) schema: ")
    table.printSchema()

    val result = engine.analyse(policy, table)

    WowLog.logInfo("Policy Engine Analyze result is :")
    logInfo(result.mkString("\n"))

    if (result.nonEmpty) WowLog.logInfo("===== Alert! =====")

    result

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
