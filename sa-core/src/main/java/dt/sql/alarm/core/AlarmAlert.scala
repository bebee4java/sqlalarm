package dt.sql.alarm.core

import tech.sqlclub.common.log.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

/**
  *
  * Created by songgr on 2019/12/25.
  */
object AlarmAlert extends Logging {

 def push(data:Dataset[AlarmRecord]): Unit = {
  logInfo("alarm alert start.....")
  val spark = data.sparkSession

  import spark.implicits._
  val itemIds = data.groupBy(col(AlarmRecord.item_id)).count().map{
   row =>
    (row.getAs[String](AlarmRecord.item_id), row.getAs[Long]("count"))
  }.collect()

  logInfo(s"alarm batch info (itemId, count):\n${itemIds.mkString("\n")}")

  itemIds.map{
   case (itemId, _) =>
    val table = data.filter(col(AlarmRecord.item_id) === itemId)

    table
  }



 }

}
