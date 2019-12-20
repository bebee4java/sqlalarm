package dt.sql.alarm.input

import dt.sql.alarm.core.Base
import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseInput extends Base {

  def getDataSetStream(spark:SparkSession):Dataset[Row]

}
