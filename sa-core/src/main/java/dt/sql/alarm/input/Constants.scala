package dt.sql.alarm.input

object Constants {

  val TOPIC_NAME = "topic"
  val SOURCE_NAME = "source"
  val VALUE_NAME = "value"

  val KAFKA_TOPIC = "kafka.topic"
  val KAFKA_SUBSCRIBE_TOPIC_PATTERN = "kafka.subscribe.topic.pattern"

  object SubscribeType extends Enumeration{
    type SubscribeType = Value
    val assign = Value(0, "assign")
    val subscribe = Value(1, "subscribe")
    val subscribePattern = Value(2,"subscribePattern")

    override def toString(): String = {
     s"{0:$assign, 1:$subscribe, 2:$subscribePattern}"
    }
  }

  val KAFKA_SERVERS = "kafka.bootstrap.servers"
  val KAFKA_GROUP = "kafka.group"
  val KAFKA_DEFAULT_GROUP = "sqlalarm_kafka_group"


  val REDIS_KEYS = "redis.keys"
  val REDIS_GROUP = "redis.group"
  val REDIS_DEFAULT_GROUP = "sqlalarm_redis_group"
  val REDIS_START_OFFSETS = "redis.start.offsets"
  val REDIS_CONSUMER_PREFIX = "redis.consumer.prefix"
  val REDIS_STREAM_PARALLELISM = "redis.stream.parallelism"
  val REDIS_STREAM_BATCH_SIZE = "redis.stream.batch.size"
  val REDIS_STREAM_READ_BLOCK_MSEC = "redis.stream.read.block.msec"

}
