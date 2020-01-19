package dt.sql.alarm.core

import dt.sql.alarm.reduce.EngineResult
import tech.sqlclub.common.log.Logging

object AlarmAlert extends Logging {


  def push(results:Array[EngineResult]) : Unit = {


    results.filter(_.hasWarning).foreach{
      result =>
        logInfo(result.toString)
    }

  }

}
