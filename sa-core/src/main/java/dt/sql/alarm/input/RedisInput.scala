package dt.sql.alarm.input
import dt.sql.alarm.conf.Conf
import dt.sql.alarm.core.Source
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  *
  * Created by songgr on 2019/12/20.
  */

@Source(name = "redis")
class RedisInput extends BaseInput {
  override def getDataSetStream(spark: SparkSession): Dataset[Row] = null

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
