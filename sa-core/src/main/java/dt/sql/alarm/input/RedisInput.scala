package dt.sql.alarm.input

import dt.sql.alarm.conf.RedisConf
import dt.sql.alarm.core.Source
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import dt.sql.alarm.input.Constants._
import dt.sql.alarm.core.Constants._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import tech.sqlclub.common.exception.SQLClubException
import tech.sqlclub.common.log.Logging
import tech.sqlclub.common.utils.ConfigUtils

/**
  *
  * Created by songgr on 2019/12/20.
  */

@Source(name = "redis")
class RedisInput extends BaseInput with Logging {
  @transient private var dStream:Dataset[Row] = _

  override def getDataSetStream(spark: SparkSession): Dataset[Row] = {
    process(spark)
    dStream
  }

  /**
    * 配置检查
    */
  override protected[this] def checkConfig: Option[RedisConf] = {
    val keys = ConfigUtils.getStringValue(s"$INPUT_PREFIX.$REDIS_KEYS")
    val group = ConfigUtils.getStringValue(s"$INPUT_PREFIX.$REDIS_GROUP", REDIS_DEFAULT_GROUP)
    val offsets = ConfigUtils.getStringValue(s"$INPUT_PREFIX.$REDIS_START_OFFSETS")
    val consumer_prefix = ConfigUtils.getStringValue(s"$INPUT_PREFIX.$REDIS_CONSUMER_PREFIX")
    val parallelism = ConfigUtils.getIntValue(s"$INPUT_PREFIX.$REDIS_STREAM_PARALLELISM")
    val batch_size = ConfigUtils.getIntValue(s"$INPUT_PREFIX.$REDIS_STREAM_BATCH_SIZE")
    val block_msec = ConfigUtils.getLongValue(s"$INPUT_PREFIX.$REDIS_STREAM_READ_BLOCK_MSEC")

    val isValid = StringUtils.isNoneBlank(keys)

    if (!isValid) throw new SQLClubException(s"$REDIS_KEYS is needed in redis input conf and cant be empty!")

    val conf = RedisConf(keys,offsets,group,consumer_prefix)
    if (parallelism > 0) conf.parallelism = parallelism
    if (batch_size > 0) conf.batch_size = batch_size
    if (block_msec > 0) conf.read_block_msec = block_msec

    Some(conf)
  }

  /**
    * 数据处理
    *
    * @param session SparkSession
    */
  override protected[this] def process(session: SparkSession): Unit = {
    logInfo("Alarm redis source process....")
    val conf = checkConfig
    if (conf.isDefined) {
      val redisConf = conf.get

      var options = Map("stream.keys" -> redisConf.keys,
        "stream.group.name" -> redisConf.group,
        "stream.parallelism" -> redisConf.parallelism,
        "stream.read.batch.size" -> redisConf.batch_size,
        "stream.read.block" -> redisConf.read_block_msec
      )

      if (redisConf.consumer_prefix != null && redisConf.consumer_prefix.nonEmpty)
        options += ("stream.consumer.prefix" -> redisConf.consumer_prefix)

      if (redisConf.start_offsets != null && redisConf.start_offsets.nonEmpty)
        options += ("stream.offsets" -> redisConf.start_offsets)

      val lines = session.readStream
        .format(fullFormat)
        .options(options.map(kv => (kv._1, kv._2.toString)))
        .schema(StructType(Array(                 // stream fields
          StructField("_id", StringType),
          StructField("key", StringType),
          StructField("value", StringType)
        )))
        .load()

      dStream = lines.selectExpr(s"'${shortFormat}' as ${source}", s"CAST(key AS STRING) as ${topic}", s"CAST(value AS STRING) as ${value}")
      logInfo("Alarm redis source process over!")
    }
  }

  override def fullFormat: String = shortFormat

  override def shortFormat: String = "redis"
}
