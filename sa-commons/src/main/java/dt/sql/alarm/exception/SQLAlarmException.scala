package dt.sql.alarm.exception

/**
  *
  * Created by songgr on 2019/12/19.
  */
class SQLAlarmException (
  val message:String,
  val cause: Throwable) extends Exception {

  def this(message:String) = this(message, null)
}
