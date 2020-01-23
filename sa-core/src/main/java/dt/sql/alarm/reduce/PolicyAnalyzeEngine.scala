package dt.sql.alarm.reduce

import dt.sql.alarm.conf.AlarmPolicyConf
import dt.sql.alarm.core.RecordDetail
import org.apache.spark.sql.{Dataset, Row, SaveMode}

/**
  * 降噪策略分析引擎
  * Created by songgr on 2020/01/09.
  */
abstract class PolicyAnalyzeEngine {

  def analyse(policy: AlarmPolicyConf, records:Dataset[Row]):(Array[EngineResult], List[(Dataset[Row], SaveMode)])
}


case class EngineResult(hasWarning:Boolean,
                        lastAlarmRecord:RecordDetail,
                        firstAlarmRecord:RecordDetail,
                        reduceCount:Int
                       )