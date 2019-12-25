package dt.sql.alarm.core

import dt.sql.alarm.log.Logging
import org.apache.spark.sql.Dataset

/**
  *
  * Created by songgr on 2019/12/25.
  */
object AlarmAlert extends Logging {

 def push(data:Dataset[AlarmRecord]): Unit = {
  logInfo("alarm alert start.....")
 }

}
