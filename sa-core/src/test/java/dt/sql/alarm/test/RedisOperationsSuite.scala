package dt.sql.alarm.test

import dt.sql.alarm.core.Constants.{appName, master}
import dt.sql.alarm.core.{RecordDetail, RedisOperations, SparkRuntime}
import org.scalatest.FunSuite
import tech.sqlclub.common.utils.{ConfigUtils, JacksonUtils}

/**
  *
  * Created by songgr on 2020/01/13.
  */
class RedisOperationsSuite extends FunSuite {

  test("rule") {
    ConfigUtils.configBuilder(Map(
      appName -> "RedisOperationsSuite",
      master -> "local[2]",
      "spark.redis.host" -> "127.0.0.1",
      "spark.redis.port" -> "6379",
      "spark.redis.db" -> "4"
    ))

    val key = "sqlalarm_rule:kafka:sqlalarm_event"
    val field = "uuid00000001"

    val value =
      """
        |{
        |    "item_id":"uuid00000001",
        |    "platform":"alarm",
        |    "title":"sql alarm test",
        |    "source":{
        |        "type":"kafka",
        |        "topic":"sqlalarm_event"
        |    },
        |    "filter":{
        |        "table":"fail_job",
        |        "structure":[
        |            {
        |                "name":"job_name",
        |                "type":"string",
        |                "xpath":"$.job_name"
        |            },
        |            {
        |                "name":"job_owner",
        |                "type":"string",
        |                "xpath":"$.job_owner"
        |            },
        |            {
        |                "name":"job_stat",
        |                "type":"string",
        |                "xpath":"$.job_stat"
        |            },
        |            {
        |                "name":"job_time",
        |                "type":"string",
        |                "xpath":"$.job_time"
        |            }
        |        ],
        |        "sql":"select job_name as job_id,job_stat,job_time as event_time,'job failed' as message, map('job_owner',job_owner) as context from fail_job where job_stat='Fail'"
        |    }
        |}
      """.stripMargin


    RedisOperations.addTableCache(key, field, value)


  }


  test("policy") {
    ConfigUtils.configBuilder(Map(
      appName -> "RedisOperationsSuite",
      master -> "local[2]",
      "spark.redis.host" -> "127.0.0.1",
      "spark.redis.port" -> "6379",
      "spark.redis.db" -> "4"
    ))

    val key = "sqlalarm_policy:kafka:sqlalarm_event"
    val field = "uuid00000001"

    val value =
      """
        |{
        | "item_id" : "uuid00000001",
        | "window": {
        |   "type": "time",
        |   "value": 10,
        |   "unit": "m"
        | },
        | "policy":{
        |   "type":"absolute",
        |   "first_alert": 1
        | }
        |}
      """.stripMargin


    RedisOperations.addTableCache(key, field, value)


  }


  test("ops") {
    ConfigUtils.configBuilder(Map(appName -> "RedisOperationsSuite", master -> "local[2]"))
    val spark = SparkRuntime.getSparkSession
    val rdd = RedisOperations.getListCache("test:111*")

    import spark.implicits._
    val ds = rdd.toDS()

    ds.printSchema()

    ds.show()

    println(ds.count())

    val tb = Seq("a","b","c").toDS()

    tb.printSchema()
    tb.show()
    println(tb.count())

    val c = tb.union(ds).count()

    println(c)


  }

}
