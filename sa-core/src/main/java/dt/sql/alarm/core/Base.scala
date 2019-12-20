package dt.sql.alarm.core

import org.apache.spark.sql.SparkSession

trait Base {
  /**
    * 配置检查
    */
  protected[this] def checkConfig():Option[Conf]

  /**
    * 数据处理
    * @param session SparkSession
    */
  protected[this] def process(session: SparkSession)

}
