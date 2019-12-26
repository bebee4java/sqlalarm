package dt.sql.alarm.core

import org.apache.spark.sql.types._

/**
  *
  * Created by songgr on 2019/12/25.
  */
case class AlarmRecord (
    job_id:String,
    job_stat:String,
    event_time:String,
    message:String,
    context:Map[String,String],
    title:String,
    platform:String,
    item_id:String
)


object AlarmRecord {
  val job_id = "job_id"
  val job_stat = "job_stat"
  val event_time = "event_time"
  val message = "message"
  val context = "context"
  val title = "title"
  val platform = "platform"
  val item_id = "item_id"

  // sql必须字段
  def getAllSQLFieldName = Seq[String](job_id, job_stat, event_time, message, context)

  // 后台自动加入的字段
  def getAllBackFieldName = Seq[String](title, platform, item_id)

  def getAllFieldName = getAllSQLFieldName ++ getAllBackFieldName

  def getAllFieldSchema = StructType(Seq(
    StructField(job_id, StringType),
    StructField(job_stat, StringType),
    StructField(event_time, StringType),
    StructField(message, StringType),
    StructField(context, MapType(StringType,StringType)),
    StructField(title, StringType),
    StructField(platform, StringType),
    StructField(item_id, StringType)
  ))
}
