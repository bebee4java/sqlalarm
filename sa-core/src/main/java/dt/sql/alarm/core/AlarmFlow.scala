package dt.sql.alarm.core

import java.util.concurrent._
import java.util

import Constants._
import dt.sql.alarm.core.Constants.{SQLALARM_ALERT, source, topic}
import dt.sql.alarm.log.Logging
import dt.sql.alarm.utils.ConfigUtils
import org.apache.spark.sql.{Dataset, Row}

object AlarmFlow extends Logging{
  val taskNum = 2
  lazy private val executors = Executors.newFixedThreadPool(taskNum)
  lazy private val taskList = new util.ArrayList[Future[Unit]](taskNum)
  lazy private val taskTimeOut = SparkRuntime.sparkConfMap.getOrElse(futureTaskTimeOut,
    ConfigUtils.getStringValue(futureTaskTimeOut, "300000")).toLong // Default timeout 5 min

  def run(data:Dataset[Row])
         (filterFunc: Array[(String,String)] => Option[Dataset[AlarmRecord]])
         (sinkFunc: Dataset[AlarmRecord] => Unit)
         (alertFunc: Dataset[AlarmRecord] => Unit) : Unit ={
    logInfo("Alarm flow start....")
    val tableIds = data.groupBy(s"$source", s"$topic").count().collect().map{
      row =>
        (row.getAs[String](s"$source"), row.getAs[String](s"$topic"), row.getAs[Long]("count"))
    }
    logInfo(s"batch info (source, topic, count):\n${tableIds.mkString("\n")}")

    var filterTable:Option[Dataset[AlarmRecord]] = null

    if (tableIds.nonEmpty){
      // sql filter
      logInfo("AlarmFlow table filter...")
      filterTable = filterFunc(tableIds.map(it => (it._1, it._2)))
      logInfo("AlarmFlow table filter pass!")
    }

    if (filterTable != null && filterTable.isDefined) {
      val table = filterTable.get

      try {
        table.persist()

        // alarm data sink
        val sinkTask = executors.submit(new Callable[Unit] {
          override def call(): Unit ={
            logInfo("AlarmFlow table sink...")
            sinkFunc(table)
            logInfo("AlarmFlow table sink task will be executed in the future!")
          }
        })
        taskList.add(sinkTask)

        // alarm record alert
        if (ConfigUtils.hasConfig(SQLALARM_ALERT)){

          val alertTask = executors.submit(new Callable[Unit] {
            override def call(): Unit ={
              logInfo("AlarmFlow table alert...")
              alertFunc(table)
              logInfo("AlarmFlow table alert task will be executed in the future!")
            }
          })
          taskList.add(alertTask)
        }

        val tasks = taskList.iterator()
        logInfo(s"We will run ${taskList.size()} tasks...")
        while (tasks.hasNext){
          val task = tasks.next()
          if (runTask(task)) tasks.remove()
        }
        logInfo(s"All task completed! Current task list number is: ${taskList.size()}.")

      } finally {
        table.unpersist()
      }
    }
    logInfo("Alarm flow end!")
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
      logInfo(s"There are ${unfinishedTasks.size} outstanding tasks to be executed...")
      val tasks = unfinishedTasks.iterator()
      while (tasks.hasNext){
        val task = tasks.next()
        if (runTask(task)) tasks.remove()
      }
      logInfo(s"All task completed! Current task list number is: ${unfinishedTasks.size()}.")
      executors.shutdownNow()
    }
  }

}
