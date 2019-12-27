package dt.sql.alarm.core

object Constants {

  val appName = "sqlalarm.name"

  val master = "sqlalarm.master"

  val source = "source"

  val topic = "topic"

  val value = "value"

  val checkpoint = "sqlalarm.checkpointLocation"

  val trigger = "spark.streaming.trigger.time.interval.msec"
  val futureTaskTimeOut = "spark.streaming.future.task.timeout.msec"

  val SQLALARM_SOURCES = "sqlalarm.sources"
  val SQLALARM_SINKS = "sqlalarm.sinks"
  val SQLALARM_ALERT = "sqlalarm.alert"

  val INPUT_PREFIX = "sqlalarm.input"
  val OUTPUT_PREFIX = "sqlalarm.output"

  val ALARM_RULE = "sqlalarm_rule"
  val ALARM_CACHE = "sqlalarm_cache"
  val ALARM_POLICY = "sqlalarm_policy"

}
