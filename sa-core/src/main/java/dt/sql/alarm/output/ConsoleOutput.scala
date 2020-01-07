package dt.sql.alarm.output
import java.util.concurrent.atomic.AtomicBoolean

import dt.sql.alarm.conf.Conf
import dt.sql.alarm.core.{AlarmRecord, Sink}
import tech.sqlclub.common.log.Logging
import tech.sqlclub.common.utils.ConfigUtils
import org.apache.spark.sql.{Dataset, SparkSession}


@Sink(name = "console")
class ConsoleOutput extends BaseOutput with Logging {
  var runtimeConfig:Map[String,String] = _
  var numRows = 20
  var truncate = true
  var flag = new AtomicBoolean(false)
  logInfo("Console sink initialization......")

  override protected[this] def checkConfig(): Option[Conf] = None


  override protected[this] def process(session: SparkSession): Unit = {
    if (!flag.get) {
      flag.synchronized {
        if (!flag.get) {
          runtimeConfig = session.conf.getAll
          numRows = runtimeConfig.getOrElse(Constants.showNumRows,
            ConfigUtils.getStringValue(Constants.showNumRows, "20")).toInt
          truncate = runtimeConfig.getOrElse(Constants.showTruncate,
            ConfigUtils.getStringValue(Constants.showTruncate, "true")).toBoolean
          flag.set(true)
        }
      }
    }
  }

  override def process(data: Dataset[AlarmRecord]): Unit = {
    process(data.sparkSession)
    logInfo("Alarm console sink process....")
    data.show(numRows, truncate)
    logInfo("Alarm console sink process over!")
  }

  override def fullFormat: String = shortFormat

  override def shortFormat: String = "console"
}
