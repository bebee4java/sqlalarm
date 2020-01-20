package dt.sql.alarm.test

import dt.sql.alarm.SQLAlarmBoot

object SQLAlarmBootTest {

  def main(args: Array[String]): Unit = {
    SQLAlarmBoot.main(
      Array(
        "-sqlalarm.master", "local[*]",
        "-sqlalarm.name", "sqlalarm",
        "-spark.redis.host", "127.0.0.1",
        "-spark.redis.port", "6379",
        "-spark.redis.db", "4",
        "-sqlalarm.sources", "kafka",
        "-sqlalarm.input.kafka.topic", "sqlalarm_event",
        "-sqlalarm.input.kafka.subscribe.topic.pattern", "1",
        "-sqlalarm.input.kafka.bootstrap.servers", "127.0.0.1:9092",
        "-sqlalarm.sinks", "console",
        "-sqlalarm.output.kafka.topic", "sqlalarm_output",
        "-sqlalarm.output.kafka.bootstrap.servers", "127.0.0.1:9092",
        "-sqlalarm.checkpointLocation", "checkpoint",
        "sqlalarm.alert.pigeonApi", "https://dt.sqlclub/api/pigeon"

      )
    )
  }

}
