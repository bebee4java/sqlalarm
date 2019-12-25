package dt.sql.alarm.core

/**
  *
  * Created by songgr on 2019/12/25.
  */
case class AlarmRecord (
    job_id:String,
    job_stat:String,
    event_time:String,
    message:String,
    context:String,
    title:String,
    platform:String,
    item_id:String
)


object AlarmRecord {
  // sql必须字段
  def getAllSQLFieldName = Seq[String]("job_id", "job_stat", "event_time", "message", "context")

  // 后台自动加入的字段
  def getAllBackFieldName = Seq[String]("title", "platform", "item_id")

  def getAllFieldName = getAllSQLFieldName ++ getAllBackFieldName
}
