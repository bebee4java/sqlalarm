package dt.sql.alarm.input
import dt.sql.alarm.core.Conf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  *
  * Created by songgr on 2019/12/20.
  */
class RedisInput extends BaseInput {
  override def getDataSetStream(spark: SparkSession): Dataset[Row] = ???

  /**
    * 配置检查
    */
  override protected[this] def checkConfig(): Option[Conf] = ???

  /**
    * 数据处理
    *
    * @param session SparkSession
    */
  override protected[this] def process(session: SparkSession): Unit = ???
}
