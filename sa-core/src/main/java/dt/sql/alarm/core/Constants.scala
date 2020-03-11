package dt.sql.alarm.core

object Constants {

  val appName = "sqlalarm.name"

  val master = "sqlalarm.master"

  val checkpoint = "sqlalarm.checkpointLocation"

  val trigger = "spark.streaming.trigger.time.interval.msec"
  val futureTaskTimeOut = "spark.streaming.future.task.timeout.msec"
  val futureTasksThreadPoolSize = "spark.streaming.future.tasks.threadPool.size"
  val redisCacheDataPartitionNum = "spark.redis.cache.data.partition.num"

  val SQLALARM_SOURCES = "sqlalarm.sources"
  val SQLALARM_SINKS = "sqlalarm.sinks"
  val SQLALARM_ALERT = "sqlalarm.alert"

  val INPUT_PREFIX = "sqlalarm.input"
  val OUTPUT_PREFIX = "sqlalarm.output"

  val ALARM_RULE = "sqlalarm_rule"
  val ALARM_CACHE = "sqlalarm_cache"
  val ALARM_POLICY = "sqlalarm_policy"


  // SQL field name
  val SQL_FIELD_TOPIC_NAME = "topic"
  val SQL_FIELD_SOURCE_NAME = "source"
  val SQL_FIELD_VALUE_NAME = "value"
  val SQL_FIELD_EARLIEST_RECORD_NAME = "earliest_record"
  val SQL_FIELD_CURRENT_RECORD_NAME = "current_record"
  val SQL_FIELD_EARLIEST_EVENT_TIME_NAME = "earliest_event_time"
  val SQL_FIELD_CURRENT_EVENT_TIME_NAME = "current_event_time"
  val SQL_FIELD_DATAFROM_NAME = "dataFrom"
  val SQL_FIELD_CACHE_NAME = "cache"
  val SQL_FIELD_STREAM_NAME = "stream"
  val SQL_FIELD_RANK_NAME = "rank"
  val SQL_FIELD_MAXRANK_NAME = "maxRank"
  val SQL_FIELD_COUNT_NAME = "count"
  val SQL_FIELD_TOTAL_COUNT_NAME = "total_count"
  val SQL_FIELD_ALARM_COUNT_NAME = "alarm_count"
  val SQL_FIELD_ALARM_PERCENT_NAME = "alarm_percent"

  val SQL_FIELD_CACHE_DURATION = "cache_duration"
  val SQL_FIELD_CACHE_ADD_INTERVAL = "cache_add_interval"
  val SQL_FIELD_CACHE_UNTIL_TIME = "cache_until_time"


}
