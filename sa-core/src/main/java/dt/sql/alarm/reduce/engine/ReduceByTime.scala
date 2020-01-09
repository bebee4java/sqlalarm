package dt.sql.alarm.reduce.engine

import dt.sql.alarm.core.AlarmRecord
import dt.sql.alarm.reduce.PolicyAnalyzeEngine
import org.apache.spark.sql.Dataset

/**
  *
  * Created by songgr on 2020/01/09.
  */
class ReduceByTime extends PolicyAnalyzeEngine {

  override def analyse(records: Dataset[AlarmRecord]): List[EngineResult] = ???
}
