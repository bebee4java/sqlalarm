package dt.sql.alarm.core

import dt.sql.alarm.core.Constants.{source, topic}
import dt.sql.alarm.log.Logging
import org.apache.spark.sql.{Dataset, Row}

object AlarmFlow extends Logging{

  def run(data:Dataset[Row])(func:(Array[(String,String)]) => Unit): Unit ={
    val tableIds = data.groupBy(s"$source", s"$topic").count().collect().map{
      row =>
        (row.getAs[String](s"$source"), row.getAs[String](s"$topic"), row.getAs[Long]("count"))
    }
    logInfo(s"batch info (source, topic, count)\n:${tableIds.mkString("\n")}")

    func(tableIds.map(it => (it._1, it._2)))
  }

}
