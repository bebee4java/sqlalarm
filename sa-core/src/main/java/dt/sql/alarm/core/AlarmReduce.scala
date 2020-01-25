package dt.sql.alarm.core

import dt.sql.alarm.conf.AlarmPolicyConf
import tech.sqlclub.common.log.Logging
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import dt.sql.alarm.reduce.PolicyAnalyzeEngine
import dt.sql.alarm.reduce.engine.ReduceByTime
import tech.sqlclub.common.utils.JacksonUtils
import org.apache.spark.sql.functions.{col, lit}
import dt.sql.alarm.core.Constants._
import dt.sql.alarm.reduce.EngineResult
import RecordDetail._

/**
  *
  * Created by songgr on 2019/12/25.
  */
object AlarmReduce extends Logging {

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

    logInfo("AlarmAlert streamData.union(cacheData) schema: ")
    table.printSchema()

    val result = engine.analyse(policy, table)

    val engineResults = result._1
    val cacheAdding = result._2

    WowLog.logInfo("Policy Engine Analyze result is :")
    logInfo(engineResults.mkString("\n"))
    
    addCache(cacheAdding)

    engineResults
  }

  
  def addCache(cacheDfs:List[(Dataset[Row], SaveMode)]) = {
    cacheDfs.foreach{
      cache =>
        val df = cache._1
        val mode = cache._2
        val jobInfos = df.groupBy(item_id, job_id, job_stat).count().collect().map{
          row =>
            (row.getAs[String](item_id), row.getAs[String](job_id), row.getAs[String](job_stat))
        }
        jobInfos.foreach{
          jobInfo =>
            val cacheDf = df.filter(col(item_id) === jobInfo._1 and col(job_id) === jobInfo._2 and col(job_stat) === jobInfo._3)
            val key = AlarmPolicyConf.getCacheKey(jobInfo._1, jobInfo._2, jobInfo._3)
            RedisOperations.setListCache(key, cacheDf, mode)
        }
    }
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
