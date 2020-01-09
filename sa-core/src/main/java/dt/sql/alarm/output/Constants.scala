package dt.sql.alarm.output

object Constants {

  val showNumRows = "spark.show.table.numRows"
  val showTruncate = "spark.show.table.truncate"

  val jdbcUrl = "jdbc.url"
  val jdbcDriver = "jdbc.driver"
  val jdbcUser = "jdbc.user"
  val jdbcPassword = "jdbc.password"
  val jdbcTable = "jdbc.table"
  val jdbcImplClass = "jdbc.implClass"
  val jdbcNumPartitions = "jdbc.numPartitions"
  val jdbcBatchsize = "jdbc.batchsize"
  val jdbcMode = "jdbc.mode"


  val kafkaImplClass = "kafka.implClass"
  val KAFKA_ACKS = "kafka.acks"
  val KAFKA_KEY_SERIALIZER_CLASS = "key.serializer.class"
  val KAFKA_VALUE_SERIALIZER_CLASS = "value.serializer.class"
  val KAFKA_TOPIC = dt.sql.alarm.input.Constants.KAFKA_TOPIC
  val KAFKA_SERVERS = dt.sql.alarm.input.Constants.KAFKA_SERVERS
}
