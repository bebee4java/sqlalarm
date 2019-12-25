package dt.sql.alarm.filter

import dt.sql.alarm.conf.AlarmRuleConf
import dt.sql.alarm.core.AlarmRecord
import dt.sql.alarm.log.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object SQLFilter extends Logging {

  private val requireCols = AlarmRecord.getAllSQLFieldName

  def process(ruleConf:AlarmRuleConf, df: Dataset[Row]):DataFrame = {
    val spark = df.sparkSession






    null

  }
}
