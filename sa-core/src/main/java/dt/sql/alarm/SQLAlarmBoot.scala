package dt.sql.alarm

import dt.sql.alarm.core._
import core.Constants._
import tech.sqlclub.common.utils.{ConfigUtils, JacksonUtils, ParamsUtils}

object SQLAlarmBoot {

  // 5 min
  val daemonCleanInterval = 5*60*1000L

  def main(args: Array[String]): Unit = {

    val params = new ParamsUtils(args)
    ConfigUtils.configBuilder(params.getParamsMap)
    ConfigUtils.showConf()
//    require(ConfigUtils.hasConfig(appName), "Application name must be set")
    require(ConfigUtils.hasConfig(checkpoint), s"SQLAlarm stream $checkpoint must be set")
    require(ConfigUtils.hasConfig(SQLALARM_SOURCES), s"SQLAlarm stream $SQLALARM_SOURCES must be set")
    require(ConfigUtils.hasConfig(INPUT_PREFIX), s"SQLAlarm stream $INPUT_PREFIX must be set")
//    require(ConfigUtils.hasConfig(SQLALARM_SINKS), s"SQLAlarm stream $SQLALARM_SINKS must be set")
//    require(ConfigUtils.hasConfig(OUTPUT_PREFIX), s"SQLAlarm stream $OUTPUT_PREFIX must be set")

    require(ConfigUtils.hasConfig(SQLALARM_SINKS) || ConfigUtils.hasConfig(SQLALARM_ALERT),
      s"SQLAlarm stream $SQLALARM_SINKS or $SQLALARM_ALERT must be set at least one of them")

    val spark = SparkRuntime.getSparkSession

    SparkRuntime.parseProcessAndSink(spark)

    var completed = false
    if (ConfigUtils.hasConfig(SQLALARM_ALERT)) {
      val partitionNum = SparkRuntime.sparkConfMap.getOrElse(Constants.redisCacheDataPartitionNum,
        ConfigUtils.getStringValue(Constants.redisCacheDataPartitionNum, "3")).toInt

      def launchCleaner = {
        // 启动alarm cache后台清理
        WowLog.logInfo("SQLAlarm cache daemon cleaner start......")
        var batchId:Long = 1L
        while ( SparkRuntime.streamingQuery != null && SparkRuntime.streamingQuery.isActive ) {
          spark.sparkContext.setJobGroup("SQLAlarm cache clean group", s"cache-clean-batch-$batchId", true)
          val rdd = RedisOperations.getListCache(ALARM_CACHE + "*", partitionNum)
          if (rdd.count() > 0) {
            import spark.implicits._
            val cacheRecords = rdd.map{
              row =>
                JacksonUtils.fromJson[RecordDetail](row, classOf[RecordDetail])
            }.toDS

            val results = AlarmReduce.cacheReduce(cacheRecords)
            AlarmAlert.push(results, true) // Force clean cache after sending
          }
          batchId = batchId + 1
          spark.sparkContext.clearJobGroup()
          Thread.sleep(daemonCleanInterval)
        }
        if ( !SparkRuntime.streamingQuery.isActive ) completed = true
      }

      new Thread("launch-cache-cleaner-in-spark-job") {
        setDaemon(true)
        override def run(): Unit = {
          while ( !completed ) {
            try {
              launchCleaner
            }catch {
              case e:Exception =>
                e.printStackTrace()
            }
            WowLog.logInfo("SQLAlarm cache daemon cleaner exited, restarted after 60 seconds!")
            if (!completed) Thread.sleep(60000)
          }

        }
      }.start()

    }

    if ( SparkRuntime.streamingQuery != null )
      SparkRuntime.streamingQuery.awaitTermination()

    // 设置completed标志为true
    completed = true

    if (!spark.sparkContext.isStopped) spark.sparkContext.stop()

    if (spark.sparkContext.isStopped) AlarmFlow.destroy

  }

}
