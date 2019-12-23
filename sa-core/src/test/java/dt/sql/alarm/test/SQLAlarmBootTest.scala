package dt.sql.alarm.test

import dt.sql.alarm.SQLAlarmBoot

object SQLAlarmBootTest {

  def main(args: Array[String]): Unit = {
    SQLAlarmBoot.main(
      Array(
        "-sqlalarm.master", "local[*]",
        "-sqlalarm.name", "sqlalarm",
        "-redis.addresses", "127.0.0.1:6379",
        "-redis.database", "4",
        "-sqlalarm.sources", "kafka",
        "-sqlalarm.input.kafka.topic", "sqlalarm_event",
        "-sqlalarm.input.kafka.subscribe.topic.pattern", "1",
        "-sqlalarm.input.kafka.bootstrap.servers", "127.0.0.1:9092",
        "-sqlalarm.sinks", "console,kafka",
        "-sqlalarm.input.kafka.topic", "sqlalarm_output",
        "-sqlalarm.input.kafka.bootstrap.servers", "127.0.0.1:9092",

        "sqlalarm.alert.pigeonApi", "https://dt.sqlclub/api/pigeon"

      )
    )
  }

}
