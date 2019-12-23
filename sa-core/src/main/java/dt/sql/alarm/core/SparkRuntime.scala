package dt.sql.alarm.core

import dt.sql.alarm.log.Logging
import dt.sql.alarm.utils.ConfigUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import dt.sql.alarm.input.{SourceInfo, Constants => InputConstants}

object SparkRuntime extends Logging {
  private var sparkSession :SparkSession = null

  def getSparkSession:SparkSession = {
    if (sparkSession == null) {
      this.synchronized {
        if (sparkSession == null) {
          logInfo("create Spark Runtime....")
          val params = ConfigUtils.toStringMap
          val conf = new SparkConf()
          params.filter(f =>
            f._1.startsWith("spark.") ||
              f._1.startsWith("hive.")
          ).foreach { f =>
            conf.set(f._1, f._2)
          }
          conf.setAppName(ConfigUtils.getStringValue(Constants.appName))
          if (ConfigUtils.hasConfig(Constants.master)) {
            conf.setMaster(ConfigUtils.getStringValue(Constants.master))
          }
          sparkSession = SparkSession.builder().config(conf).getOrCreate()
          logInfo("Spark Runtime created!!!")
        }
      }
    }
    sparkSession
  }

  def parseProcessAndSink(spark:SparkSession) = {
    logInfo("spark parse process and sink start...")
    getSourceTable(spark).printSchema

  }

  def getSourceTable(spark:SparkSession) = {
    val sources_ = ConfigUtils.getStringValue(InputConstants.SQLALARM_SOURCES)

    val sourceNames = sources_.split(",").filterNot(_.isEmpty)

    assert(sourceNames.filterNot(SourceInfo.sourceExist(_)).size == 0,
    s"Check the configuration of sources, at present only supported: ${SourceInfo.getAllSource}"
    )

    val sources = sourceNames.map {
      sourceName =>
        SourceInfo.getSource(sourceName).getDataSetStream(spark)
    }

    sources.filter(_ != null).reduce(_.union(_))
  }
}
