package dt.sql.alarm.core

import java.util.concurrent._
import java.util

import Constants._
import dt.sql.alarm.conf.{AlarmPolicyConf, AlarmRuleConf}
import dt.sql.alarm.core.Constants.SQLALARM_ALERT
import tech.sqlclub.common.log.Logging
import tech.sqlclub.common.utils.ConfigUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object AlarmFlow extends Logging {

  def taskNum:Int = SparkRuntime.sparkConfMap.getOrElse( futureTasksThreadPoolSize,
    ConfigUtils.getStringValue(futureTasksThreadPoolSize, "2")).toInt

  lazy private val executors = Executors.newFixedThreadPool(taskNum)
  lazy private val taskList = new util.ArrayList[Future[Unit]](taskNum)
  lazy private val taskTimeOut = SparkRuntime.sparkConfMap.getOrElse(futureTaskTimeOut,
    ConfigUtils.getStringValue(futureTaskTimeOut, "300000")).toLong // Default timeout 5 min

  def run(data:Dataset[Row])
         (filterFunc: (Dataset[Row], AlarmRuleConf, AlarmPolicyConf) => Dataset[RecordDetail])
         (sinkFunc: Dataset[RecordDetail] => Unit)
         (alertFunc: (Dataset[RecordDetail], AlarmPolicyConf) => Unit)
         (implicit spark:SparkSession = data.sparkSession):Unit = {

    WowLog.logInfo("Alarm flow start....")

    import spark.implicits._
    val tableIds = data.groupBy(s"${RecordDetail.source}", s"${RecordDetail.topic}").count().map{
      row =>
        (row.getAs[String](s"${RecordDetail.source}"), row.getAs[String](s"${RecordDetail.topic}"), row.getAs[Long]("count"))
    }.collect()

    WowLog.logInfo(s"batch info (source, topic, count):\n${tableIds.mkString("\n")}")

    if (tableIds.isEmpty) {
      WowLog.logInfo("batch tableIds is empty return directly!")
      return
    }

    val rulesWithItemId:Array[(String,AlarmRuleConf)] = tableIds.flatMap{
      case (source, topic, _) =>
        val key = AlarmRuleConf.getRkey(source, topic) // rule redis key
        RedisOperations.getTableCache(Array(key)).collect() // get rules
    }.map{
      case (ruleConfId, ruleConf) =>
        (ruleConfId, AlarmRuleConf.formJson(ruleConf))
    }

    if (rulesWithItemId.isEmpty){
      WowLog.logInfo("alarm rule confs is empty return directly!")
      return
    }

    rulesWithItemId.foreach{
      item =>
        val rule = item._2  // 告警规则
        val policyConf = RedisOperations.getTableCache(AlarmPolicyConf.getRkey(rule.source.`type`, rule.source.topic), rule.item_id)
        val policy = AlarmPolicyConf.formJson(policyConf) //告警策略
        // sql filter
        WowLog.logInfo("AlarmFlow table filter...")
        val filterTable = filterFunc(data, rule, policy)
        WowLog.logInfo("AlarmFlow table filter pass!")


        sinkAndAlert(filterTable, sinkFunc, alertFunc){
          () =>
            val tasks = taskList.iterator()
            WowLog.logInfo(s"We will run ${taskList.size()} tasks...")
            while (tasks.hasNext){
              val task = tasks.next()
              if (runTask(task)) tasks.remove()
            }
            WowLog.logInfo(s"All task completed! Current task list number is: ${taskList.size()}.")
        }(rule, policy)

    }
    WowLog.logInfo("Alarm flow end!")
  }

  def sinkAndAlert(filterTable:Dataset[RecordDetail],
                   sinkFunc:Dataset[RecordDetail]=>Unit,
                   alertFunc:(Dataset[RecordDetail],AlarmPolicyConf)=>Unit)(run:()=>Unit)
                  (implicit ruleConf: AlarmRuleConf, policyConf: AlarmPolicyConf): Unit ={
    try {
      filterTable.persist()
      if (filterTable.count() == 0) {
        WowLog.logInfo("filterTable is empty, don't need to need sink and alert functions return directly!")
        return
      }

      // alarm data sink
      val sinkTask = executors.submit(new Callable[Unit] {
        override def call(): Unit ={
          WowLog.logInfo("AlarmFlow table sink...")
          sinkFunc(filterTable)
          WowLog.logInfo("AlarmFlow table sink task will be executed in the future!")
        }
      })
      taskList.add(sinkTask)

      // alarm record alert
      if (ConfigUtils.hasConfig(SQLALARM_ALERT)){

        val alertTask = executors.submit(new Callable[Unit] {
          override def call(): Unit ={
            WowLog.logInfo("AlarmFlow table alert...")
            alertFunc(filterTable, policyConf)
            WowLog.logInfo("AlarmFlow table alert task will be executed in the future!")
          }
        })
        taskList.add(alertTask)
      }
      run()
    }finally {
      filterTable.unpersist()
    }
  }

  def runTask( task:Future[Unit] ): Boolean = {
    if (task != null && !task.isDone) {

      try {
        task.get(taskTimeOut, TimeUnit.MILLISECONDS)
      } catch {
        case e if e.isInstanceOf[InterruptedException] || e.isInstanceOf[ExecutionException] =>
          logError(e.getMessage, e)
        case e: TimeoutException =>
          logWarning(e.getMessage, e)
          return false
      }
    }
    true
  }

  def destroy = {
    if (executors != null) {
      import scala.collection.JavaConverters._
      val unfinishedTasks = taskList.asScala.filterNot(_.isDone).asJava
      WowLog.logInfo(s"There are ${unfinishedTasks.size} outstanding tasks to be executed...")
      val tasks = unfinishedTasks.iterator()
      while (tasks.hasNext){
        val task = tasks.next()
        if (runTask(task)) tasks.remove()
      }
      WowLog.logInfo(s"All task completed! Current task list number is: ${unfinishedTasks.size()}.")
      if (!executors.isShutdown) executors.shutdownNow()
    }
  }

}
