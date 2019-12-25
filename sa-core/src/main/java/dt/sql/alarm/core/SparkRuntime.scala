package dt.sql.alarm.core

import dt.sql.alarm.conf.AlarmRuleConf
import dt.sql.alarm.log.Logging
import dt.sql.alarm.utils.ConfigUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import dt.sql.alarm.input.SourceInfo
import Constants._
import dt.sql.alarm.filter.SQLFilter
import dt.sql.alarm.msgpiper.MsgDeliver
import org.apache.spark.sql.streaming.StreamingQuery

object SparkRuntime extends Logging {
  private var sparkSession :SparkSession = null
  var streamingQuery:StreamingQuery = null
  lazy val msgDeliver = MsgDeliver.getInstance

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
          conf.setAppName(ConfigUtils.getStringValue(appName))
          if (ConfigUtils.hasConfig(master)) {
            conf.setMaster(ConfigUtils.getStringValue(master))
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
    val sources = getSourceTable(spark)

    val dStreamWriter = sources.writeStream.foreachBatch {
      (batchTable, batchId) =>
        logInfo(s"start processing batch: $batchId")
        AlarmFlow.run(batchTable){
          batchInfo =>
            batchInfo.foreach {
              case (source, topic) =>
                val rule_rkey = AlarmRuleConf.getRkey(source, topic)
                val rule_map = msgDeliver.getTableCache(rule_rkey)
                rule_map.map{
                  case (ruleConfId, ruleConf) =>
                    val rule = AlarmRuleConf.formJson(ruleConf)
                    SQLFilter.process(rule, batchTable)
                }
            }
        }
        logInfo(s"bath $batchId processing is done.")
    }

    streamingQuery = dStreamWriter
      .queryName(ConfigUtils.getStringValue(appName))
      .option("checkpointLocation", ConfigUtils.getStringValue(checkpoint))
      .start()
  }

  def getSourceTable(spark:SparkSession) = {
    val sources_ = ConfigUtils.getStringValue(SQLALARM_SOURCES)

    val sourceNames = sources_.split(",").filterNot(_.isEmpty)

    assert(sourceNames.filterNot(SourceInfo.sourceExist(_)).size == 0,
    s"Check the configuration of sources, at present only supported: ${SourceInfo.getAllSource}"
    )

    val sources = sourceNames.map {
      sourceName =>
        SourceInfo.getSource(sourceName).getDataSetStream(spark)
    }

    sources.filter(_ != null).reduce(_ union _)
  }
}
