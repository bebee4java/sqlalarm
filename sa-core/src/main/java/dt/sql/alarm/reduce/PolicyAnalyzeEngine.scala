package dt.sql.alarm.reduce

import dt.sql.alarm.core.AlarmRecord
import org.apache.spark.sql.Dataset

/**
  * 降噪策略分析引擎
  * Created by songgr on 2020/01/09.
  */
abstract class PolicyAnalyzeEngine {

  case class EngineResult(hasWarning:Boolean,
                          lastAlarmRecord:AlarmRecord,
                          firstAlarmRecord:AlarmRecord,
                          reduceCount:Int
                         )

  def analyse(records:Dataset[AlarmRecord]):List[EngineResult]

}
