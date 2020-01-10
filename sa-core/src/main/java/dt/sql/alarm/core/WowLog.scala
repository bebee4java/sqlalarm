package dt.sql.alarm.core

import tech.sqlclub.common.log.Logging

object WowLog extends Logging {

  override def logInfo(msg: => String): Unit = {
    val info = s""" ###### $msg ###### """
    super.logInfo(info)
  }

  override def logInfo(msg: => String, throwable: Throwable): Unit = {
    val info = s""" ###### $msg ###### """
    super.logInfo(info, throwable)
  }
}
