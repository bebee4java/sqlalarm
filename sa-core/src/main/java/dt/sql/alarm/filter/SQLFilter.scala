package dt.sql.alarm.filter

import dt.sql.alarm.conf.AlarmRuleConf
import org.apache.spark.sql.{Dataset, Row}

object SQLFilter {

  def process(ruleConf:AlarmRuleConf, df: Dataset[Row]):Dataset[Row] = {
    null
  }

}
