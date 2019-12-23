package dt.sql.alarm

import dt.sql.alarm.core.{Constants, SparkRuntime}
import dt.sql.alarm.utils.{ConfigUtils, ParamsUtils}

object SQLAlarmBoot {

  def main(args: Array[String]): Unit = {

    val params = new ParamsUtils(args)
    require(params.hasParam(Constants.appName), "Application name must be set")

    ConfigUtils.configBuilder(params.getParamsMap)

    val spark = SparkRuntime.getSparkSession

    SparkRuntime.parseProcessAndSink(spark)

  }

}
