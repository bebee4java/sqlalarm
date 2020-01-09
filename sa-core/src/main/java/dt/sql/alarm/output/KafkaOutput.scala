package dt.sql.alarm.output

import java.util.concurrent.atomic.AtomicBoolean

import dt.sql.alarm.conf.KafkaConf
import dt.sql.alarm.core.{AlarmRecord, Sink}
import org.apache.spark.sql.{Dataset, SparkSession}
import tech.sqlclub.common.log.Logging
import tech.sqlclub.common.utils.{ConfigUtils, JacksonUtils}
import dt.sql.alarm.core.Constants.OUTPUT_PREFIX
import dt.sql.alarm.output.Constants._
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import tech.sqlclub.common.exception.SQLClubException

/**
  * kafka sink
  * Created by songgr on 2020/01/08.
  */

@Sink(name = "kafka")
class KafkaOutput extends BaseOutput with Logging {
  val KAFKA_KEY_ATTRIBUTE_NAME = "key"
  val KAFKA_VALUE_ATTRIBUTE_NAME = "value"
  val KAFKA_BOOTSTRAP_SERVERS_NAME = "kafka.bootstrap.servers"
  val KAFKA_TOPIC_NAME = "topic"

  var kafkaConf:KafkaConf = _
  var flag = new AtomicBoolean(false)
  logInfo("Kafka sink initialization......")

  override def process(data: Dataset[AlarmRecord]): Unit = {
    val spark = data.sparkSession
    process(spark)
    logInfo("Alarm Kafka sink process....")

    val format = ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$kafkaImplClass", fullFormat)
    var options = Map(KAFKA_BOOTSTRAP_SERVERS_NAME -> kafkaConf.servers,
      KAFKA_TOPIC_NAME -> kafkaConf.topic
    )
    options += (ProducerConfig.ACKS_CONFIG -> ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$KAFKA_ACKS", "-1"))
    options += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG ->
      ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$KAFKA_KEY_SERIALIZER_CLASS", classOf[StringSerializer].getName))
    options += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG ->
      ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$KAFKA_VALUE_SERIALIZER_CLASS", classOf[StringSerializer].getName))

    import spark.implicits._
      data.map{
      record =>
        (StringUtils.join(Array(record.job_id,record.job_stat), ":")
          , JacksonUtils.toJson(record)
        )
    }.toDF(KAFKA_KEY_ATTRIBUTE_NAME,KAFKA_VALUE_ATTRIBUTE_NAME).write
      .format(format).options(options).mode("append").save()

    logInfo("Alarm Kafka sink process over!")
  }

  override def fullFormat: String = shortFormat

  override def shortFormat: String = "kafka"

  /**
    * 配置检查
    */
  override protected[this] def checkConfig(): Option[KafkaConf] = {
    val topic = ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$KAFKA_TOPIC")
    val servers = ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$KAFKA_SERVERS")

    val isValid = StringUtils.isNoneBlank(topic) &&
      StringUtils.isNoneBlank(servers)

    if (!isValid) {
      throw new SQLClubException(s"$KAFKA_TOPIC and $KAFKA_SERVERS are needed in kafka sink conf and cant be empty!")
    }

    Some(KafkaConf(null, topic, servers, null))
  }

  /**
    * 数据处理
    *
    * @param session SparkSession
    */
  override protected[this] def process(session:  SparkSession): Unit = {
    if (!flag.get) {
      flag.synchronized {
        if (!flag.get) {
          kafkaConf = checkConfig.get
          flag.set(true)
        }
      }
    }
  }
}
