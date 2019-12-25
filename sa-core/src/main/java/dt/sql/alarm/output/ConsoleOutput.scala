package dt.sql.alarm.output
import dt.sql.alarm.conf.Conf
import dt.sql.alarm.core.{AlarmRecord, Sink}
import dt.sql.alarm.log.Logging
import org.apache.spark.sql.{Dataset, SparkSession}


@Sink(name = "console")
object ConsoleOutput extends BaseOutput with Logging {
  var runtimeConfig:Map[String,String] = _

  override protected[this] def checkConfig(): Option[Conf] = None


  override protected[this] def process(session: SparkSession): Unit = {
    runtimeConfig = session.conf.getAll
  }

  override def process(data: Dataset[AlarmRecord]): Unit = {
    logInfo("Alarm console sink process....")
    val numRows = runtimeConfig.getOrElse(Constants.showNumRows, "20")
    val truncate = runtimeConfig.getOrElse(Constants.showTruncate, "true")
    data.show(numRows.toInt, truncate.toBoolean)
    logInfo("Alarm console sink process over!")
  }

}
