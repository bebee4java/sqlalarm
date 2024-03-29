package dt.sql.alarm.core

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import dt.sql.alarm.input.SourceInfo
import Constants._
import dt.sql.alarm.filter.SQLFilter
import dt.sql.alarm.output.SinkInfo
import dt.sql.alarm.reduce.EngineResult
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import tech.sqlclub.common.log.Logging
import tech.sqlclub.common.utils.ConfigUtils
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import tech.sqlclub.common.exception.SQLClubException
import scala.collection.JavaConverters._

object SparkRuntime extends Logging {
  private var sparkSession :SparkSession = null
  var sparkConfMap:Map[String,String] = null
  var streamingQuery:StreamingQuery = null

  def getSparkSession:SparkSession = {
    if (sparkSession == null) {
      this.synchronized {
        if (sparkSession == null) {
          WowLog.logInfo("create Spark Runtime....")
          val params = ConfigUtils.toStringMap
          val conf = new SparkConf()
          params.filter(f =>
            f._1.startsWith("spark.") ||
              f._1.startsWith("hive.")
          ).foreach { f =>
            conf.set(f._1, f._2)
          }
          if (ConfigUtils.hasConfig(appName)) {
            conf.setAppName(ConfigUtils.getStringValue(appName))
          }
          if (ConfigUtils.hasConfig(master)) {
            conf.setMaster(ConfigUtils.getStringValue(master))
          }
          sparkSession = SparkSession.builder().config(conf).getOrCreate()
          sparkConfMap = sparkSession.conf.getAll
          WowLog.logInfo("Spark Runtime created!!!")
        }
      }
    }
    sparkSession
  }

  def parseProcessAndSink(spark:SparkSession) = {
    WowLog.logInfo("spark parse process and sink start...")
    val sources = getSourceTable(spark)
    WowLog.logInfo("spark stream get all source table succeed!")
    logInfo("All source data schema: ")
    sources.printSchema()
    val dStreamWriter = sources.writeStream.foreachBatch{
      (batchTable, batchId) =>
        WowLog.logInfo(s"start processing batch: $batchId")
        val start = System.nanoTime()
        AlarmFlow.run(batchId, batchTable){
          // filterFunc
          (table, rule, policy) =>
            val filterTable = SQLFilter.process(table, rule, policy)
            import spark.implicits._
            filterTable.as[RecordDetail]
        }{
          // sinkFunc
          table =>
            sinks.foreach(_ process table.filter(_.alarm == 1) )
        }{
          // alertFunc
          (table, policy)=>
            val alarmRecords = if (null != policy) {
              AlarmReduce.reduce(table, policy) // alarm noise reduction
            } else {
              // 没配置策略每条都push
              table.collect().map{
                record =>
                  EngineResult(true, record, record, 1)
              }
            }
            AlarmAlert.push(alarmRecords) // alarm alert
        }
        val end = System.nanoTime()
        WowLog.logInfo(s"bath $batchId processing is done. Total time consuming: ${(end-start)/1000000} ms.")
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
/*
    root
    |-- source: string (nullable = false)
    |-- topic: string (nullable = false)
    |-- value: string (nullable = false)
*/
    sources.filter(_ != null).reduce(_ union _)
  }
}

object RedisOperations {
  import redis.clients.jedis.Jedis
  import com.redislabs.provider.redis._
  import redis.clients.jedis.ScanParams
  import com.redislabs.provider.redis.util.ConnectionUtils

  lazy private val spark = SparkRuntime.getSparkSession
  def sc:SparkContext = spark.sparkContext

  lazy private val redisEndpoint = RedisConfig.fromSparkConf(SparkEnv.get.conf).initialHost
  lazy private val readWriteConfig = ReadWriteConfig.fromSparkConf(SparkEnv.get.conf)

  def IncorrectMsg = s"RedisOperations keysOrKeyPattern should be String or Array[String]"

  def getTableCache[T](keysOrKeyPattern: T, partitionNum:Int):RDD[(String, String)] = {
    keysOrKeyPattern match {
      case keyPattern: String => sc.fromRedisHash(keyPattern.asInstanceOf[String], partitionNum)
      case keys: Array[String] => sc.fromRedisHash(keys.asInstanceOf[Array[String]], partitionNum)
      case _ => throw new SQLClubException(IncorrectMsg)
    }
  }

  def getTableCache[T](keysOrKeyPattern: T):RDD[(String, String)] = getTableCache(keysOrKeyPattern, 3)

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


  def getListCache[T](keysOrKeyPattern:T, partitionNum:Int=3):RDD[String] = {
    keysOrKeyPattern match {
      case keyPattern: String => sc.fromRedisList(keyPattern.asInstanceOf[String], partitionNum)
      case keys: Array[String] => sc.fromRedisList(keys.asInstanceOf[Array[String]], partitionNum)
      case _ => throw new SQLClubException(IncorrectMsg)
    }

  }

  def scanListCacheKeys(keyPattern:String)
                       (implicit conn:Jedis = redisEndpoint.connect(), config:ReadWriteConfig = readWriteConfig):Seq[String]= {
    ConnectionUtils.withConnection[Seq[String]](conn) {
      conn =>
        val keys = new java.util.ArrayList[String]
        val params = new ScanParams().`match`(keyPattern).count(config.scanCount)
        var cursor = "0"
        do {
          val scan = conn.scan(cursor, params)
          keys.addAll(scan.getResult)
          cursor = scan.getCursor
        } while (cursor != "0")
        keys.asScala
    }
  }

  def setListCache[T](key:String, data:T, saveMode: SaveMode, ttl:Int=0) = {
    if (SaveMode.Overwrite == saveMode) {
      val conn = redisEndpoint.connect()
      ConnectionUtils.withConnection[Long](conn) {
        conn =>
          conn.del(key)
      }
    }
    import spark.implicits._
    val rdd = data match {
      case rdd:RDD[String] => rdd.filter(s => s != null && s.nonEmpty).map(s => s.toString)
      case df:DataFrame => df.filter(_ != null).map(row => row.getAs[String](0)).rdd
    }

    sc.toRedisLIST(rdd, key, ttl)
  }

  def delCache(keys:String*)
    (implicit conn:Jedis = redisEndpoint.connect()): Long = {
    ConnectionUtils.withConnection[Long](conn) {
      conn =>
        conn.del(keys:_*)
    }
  }

}
