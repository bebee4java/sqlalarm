package dt.sql.alarm

import dt.sql.alarm.core.{AlarmFlow, SparkRuntime}
import core.Constants._
import tech.sqlclub.common.utils.{ConfigUtils, ParamsUtils}

object SQLAlarmBoot {

  def main(args: Array[String]): Unit = {

    val params = new ParamsUtils(args)
    ConfigUtils.configBuilder(params.getParamsMap)
    ConfigUtils.showConf()
    require(ConfigUtils.hasConfig(appName), "Application name must be set")
    require(ConfigUtils.hasConfig(checkpoint), s"SQLAlarm stream $checkpoint must be set")
    require(ConfigUtils.hasConfig(SQLALARM_SOURCES), s"SQLAlarm stream $SQLALARM_SOURCES must be set")
    require(ConfigUtils.hasConfig(INPUT_PREFIX), s"SQLAlarm stream $INPUT_PREFIX must be set")
//    require(ConfigUtils.hasConfig(SQLALARM_SINKS), s"SQLAlarm stream $SQLALARM_SINKS must be set")
//    require(ConfigUtils.hasConfig(OUTPUT_PREFIX), s"SQLAlarm stream $OUTPUT_PREFIX must be set")

    require(ConfigUtils.hasConfig(SQLALARM_SINKS) || ConfigUtils.hasConfig(SQLALARM_ALERT),
      s"SQLAlarm stream $SQLALARM_SINKS or $SQLALARM_ALERT must be set at least one of them")

    val spark = SparkRuntime.getSparkSession

    SparkRuntime.parseProcessAndSink(spark)

    if ( SparkRuntime.streamingQuery != null )
      SparkRuntime.streamingQuery.awaitTermination()

    if (!spark.sparkContext.isStopped) spark.sparkContext.stop()

    if (spark.sparkContext.isStopped) AlarmFlow.destroy

  }

}
