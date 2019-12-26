package dt.sql.alarm.core

import dt.sql.alarm.core.Constants.{SQLALARM_ALERT, source, topic}
import dt.sql.alarm.log.Logging
import dt.sql.alarm.utils.ConfigUtils
import org.apache.spark.sql.{Dataset, Row}

object AlarmFlow extends Logging{

  def run(data:Dataset[Row])
         (filterFunc: Array[(String,String)] => Option[Dataset[AlarmRecord]])
         (sinkFunc: Dataset[AlarmRecord] => Unit)
         (alertFunc: Dataset[AlarmRecord] => Unit) : Unit ={
    logInfo("Alarm flow start....")
    val tableIds = data.groupBy(s"$source", s"$topic").count().collect().map{
      row =>
        (row.getAs[String](s"$source"), row.getAs[String](s"$topic"), row.getAs[Long]("count"))
    }
    logInfo(s"batch info (source, topic, count):\n${tableIds.mkString("\n")}")

    var filterTable:Option[Dataset[AlarmRecord]] = null

    if (tableIds.nonEmpty){
      // sql filter
      logInfo("AlarmFlow table filter...")
      filterTable = filterFunc(tableIds.map(it => (it._1, it._2)))
      logInfo("AlarmFlow table filter pass!")
    }

    if (filterTable != null && filterTable.isDefined) {
      val table = filterTable.get
      try {
        table.persist()
        // alarm data sink
        logInfo("AlarmFlow table sink...")
        sinkFunc(table)
        logInfo("AlarmFlow table sink pass!")
        // alarm record alert
        if (ConfigUtils.hasConfig(SQLALARM_ALERT)){
          logInfo("AlarmFlow table alert...")
          alertFunc(table)
          logInfo("AlarmFlow table pass...")
        }
      } finally {
        table.unpersist()
      }
    }
    logInfo("Alarm flow end!")
  }

}
