package dt.sql.alarm.output

import java.util.concurrent.atomic.AtomicBoolean

import dt.sql.alarm.conf.JdbcConf
import dt.sql.alarm.core.{AlarmRecord, Sink}
import org.apache.spark.sql.{Dataset, SparkSession}
import tech.sqlclub.common.log.Logging
import tech.sqlclub.common.utils.{ConfigUtils, JacksonUtils}
import dt.sql.alarm.core.Constants._
import dt.sql.alarm.output.Constants._
import org.apache.commons.lang3.StringUtils
import tech.sqlclub.common.exception.SQLClubException

/**
  * jdbc sink
  * Created by songgr on 2020/01/06.
  */
@Sink(name = "jdbc")
class JdbcOutput extends BaseOutput with Logging  {
  var jdbcConf:JdbcConf = _
  var flag = new AtomicBoolean(false)
  logInfo("JDBC sink initialization......")

  override def fullFormat: String = shortFormat

  override def shortFormat: String = "jdbc"

  override def process(data: Dataset[AlarmRecord]): Unit = {
    process(data.sparkSession)
    logInfo("Alarm JDBC sink process....")

    val format = ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$jdbcImplClass", fullFormat)

    val json = JacksonUtils.toJson(jdbcConf)
    val options = JacksonUtils.fromJson(json, classOf[Map[String,AnyRef]]).map(kv => (kv._1, kv._2.toString))

    import org.apache.spark.sql.functions._
    val table = data.withColumn(AlarmRecord.context, to_json(col(AlarmRecord.context)))

    table.write.format(format).options(options).mode(jdbcConf.mode).save(jdbcConf.dbtable)

    logInfo("Alarm JDBC sink process over!")

  }

  /**
    * 配置检查
    */
  override protected[this] def checkConfig(): Option[JdbcConf] = {
    val url = ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$jdbcUrl")
    val driver = ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$jdbcDriver")
    val user = ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$jdbcUser")
    val password = ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$jdbcPassword")
    val table = ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$jdbcTable")
    val numPartitions = ConfigUtils.getIntValue(s"$OUTPUT_PREFIX.$jdbcNumPartitions")
    val batchsize = ConfigUtils.getIntValue(s"$OUTPUT_PREFIX.$jdbcBatchsize")
    val mode = ConfigUtils.getStringValue(s"$OUTPUT_PREFIX.$jdbcMode")

    val isValid = StringUtils.isNoneBlank(url) &&
      StringUtils.isNoneBlank(driver) &&
      StringUtils.isNoneBlank(user)

    if (!isValid) {
      throw new SQLClubException(s"$jdbcUrl and $jdbcDriver and $jdbcUser are needed in jdbc sink conf and cant be empty!")
    }

    val conf = JdbcConf(url, driver, user, password)
    if (StringUtils.isNoneBlank(table))
      conf.dbtable = table
    if (numPartitions > 0)
      conf.numPartitions = numPartitions
    if (batchsize > 0)
      conf.batchsize = batchsize
    if (StringUtils.isNoneBlank(mode))
      conf.mode = mode

    Some(conf)
  }

  /**
    * 数据处理
    *
    * @param session SparkSession
    */
  override protected[this] def process(session:SparkSession): Unit = {
    if (!flag.get) {
      flag.synchronized {
        if (!flag.get) {
          jdbcConf = checkConfig.get
          flag.set(true)
        }
      }
    }
  }
}
