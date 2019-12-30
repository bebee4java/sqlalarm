package dt.sql.alarm.core

import dt.sql.alarm.conf.AlarmRuleConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import dt.sql.alarm.input.SourceInfo
import Constants._
import dt.sql.alarm.filter.SQLFilter
import dt.sql.alarm.output.SinkInfo
import org.apache.spark.rdd.RDD
import tech.sqlclub.common.log.Logging
import tech.sqlclub.common.utils.ConfigUtils
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object SparkRuntime extends Logging {
  private var sparkSession :SparkSession = null
  var sparkConfMap:Map[String,String] = null
  var streamingQuery:StreamingQuery = null

  def getSparkSession:SparkSession = {
    if (sparkSession == null) {
      this.synchronized {
        if (sparkSession == null) {
          logInfo("create Spark Runtime....")
          val params = ConfigUtils.toStringMap
          val conf = new SparkConf()
          params.filter(f =>
            f._1.startsWith("spark.") ||
              f._1.startsWith("hive.")
          ).foreach { f =>
            conf.set(f._1, f._2)
          }
          conf.setAppName(ConfigUtils.getStringValue(appName))
          if (ConfigUtils.hasConfig(master)) {
            conf.setMaster(ConfigUtils.getStringValue(master))
          }
          sparkSession = SparkSession.builder().config(conf).getOrCreate()
          sparkConfMap = sparkSession.conf.getAll
          logInfo("Spark Runtime created!!!")
        }
      }
    }
    sparkSession
  }

  def parseProcessAndSink(spark:SparkSession) = {
    logInfo("spark parse process and sink start...")
    val sources = getSourceTable(spark)
    logInfo("spark stream get all source table succeed!")
    sources.printSchema()
    val dStreamWriter = sources.writeStream.foreachBatch {
      (batchTable, batchId) =>
        logInfo(s"start processing batch: $batchId")
        AlarmFlow.run(batchTable){
          batchInfo =>
            val allTable = batchInfo.flatMap {
              case (source, topic) =>
                val rule_rkey = AlarmRuleConf.getRkey(source, topic)
                val rule_map = RedisOperations.getTableCache(Array(rule_rkey)).collect()
                val tables = rule_map.map{
                  case (ruleConfId, ruleConf) =>
                    val rule = AlarmRuleConf.formJson(ruleConf)
                    SQLFilter.process(rule, batchTable)
                }
                tables.map {
                  df =>
                    import spark.implicits._
                    df.as[AlarmRecord]
                }
            }
            if (allTable.nonEmpty) {
              val unionTable = allTable.reduce(_ union _)
              scala.Some(unionTable)
            } else {
              None
            }
        }{
          table =>
            sinks.map(_.process(table))
        } {
          table =>
           AlarmAlert.push(table)
        }
        logInfo(s"bath $batchId processing is done.")
    }

    streamingQuery = dStreamWriter
      .queryName(ConfigUtils.getStringValue(appName))
      .option("checkpointLocation", ConfigUtils.getStringValue(checkpoint))
      .trigger(Trigger.ProcessingTime(sparkConfMap.getOrElse(trigger,
        ConfigUtils.getStringValue(trigger, "3000")).toLong)) // 默认3s
      .start()
  }

  private lazy val sinks = getSinks

  def getSinks = {
    val sinks = ConfigUtils.getStringValue(SQLALARM_SINKS)
    val sinkNames = sinks.split(",").filterNot(_.isEmpty)

    assert(sinkNames.filterNot(SinkInfo.sinkExist(_)).size == 0,
      s"Check the configuration of sink, at present only supported: ${SinkInfo.getAllSink}"
    )
    sinkNames.map(SinkInfo.getSink(_))
  }

  def getSourceTable(spark:SparkSession) = {
    val sources_ = ConfigUtils.getStringValue(SQLALARM_SOURCES)

    val sourceNames = sources_.split(",").filterNot(_.isEmpty)

    assert(sourceNames.filterNot(SourceInfo.sourceExist(_)).size == 0,
    s"Check the configuration of sources, at present only supported: ${SourceInfo.getAllSource}"
    )

    val sources = sourceNames.map {
      sourceName =>
        logInfo(s"spark stream create source $sourceName!")
        SourceInfo.getSource(sourceName).getDataSetStream(spark)
    }

    sources.filter(_ != null).reduce(_ union _)
  }
}

object RedisOperations {
  import redis.clients.jedis.Jedis
  import com.redislabs.provider.redis._
  import com.redislabs.provider.redis.util.ConnectionUtils
  lazy private val sc = SparkRuntime.getSparkSession.sparkContext

  lazy private val redisEndpoint = RedisConfig.fromSparkConf(sc.getConf).initialHost

  def getTableCache[T](keysOrKeyPattern: T):RDD[(String, String)] = {
    sc.fromRedisHash(keysOrKeyPattern)
  }

  def getTableCache(key: String, field:String)
    (implicit conn:Jedis = redisEndpoint.connect()):String = {
    ConnectionUtils.withConnection[String](conn) {
      conn =>
        conn.hget(key, field)
    }
  }


  def addTableCache(key: String, field: String, value: String)
    (implicit conn:Jedis = redisEndpoint.connect()): Long = {
    ConnectionUtils.withConnection[Long](conn) {
      conn =>
        conn.hset(key, field, value)
    }
  }

}
