package dt.sql.alarm.test

import dt.sql.alarm.core.Constants.{ALARM_CACHE, appName, master}
import dt.sql.alarm.core.{RecordDetail, RedisOperations, SparkRuntime}
import org.apache.spark.sql.SaveMode
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
        |        "sql":"select job_name as job_id,job_stat,job_time as event_time, job_stat as message, map('job_owner',job_owner) as context from fail_job where job_stat='Fail'"
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
        |   "type":"scale",
        |   "agg":"count",
        |   "value":100,
        |   "first_alert": 1
        | }
        |}
      """.stripMargin

    val value1 =
      """
        |{
        | "item_id" : "uuid00000001",
        | "window": {
        |   "type": "time",
        |   "value": 10,
        |   "unit": "m"
        | },
        | "policy":{
        |   "type":"absolute"
        | }
        |}
      """.stripMargin

    val value2 =
      """
        |{
        | "item_id" : "uuid00000001",
        | "window": {
        |   "type": "number",
        |   "value": 4,
        |   "unit": "n"
        | },
        | "policy":{
        |   "type":"scale",
        |   "unit":"number",
        |   "value":2,
        |   "first_alert": 1
        | }
        |}
      """.stripMargin

    val value3 =
      """
        |{
        | "item_id" : "uuid00000001",
        | "window": {
        |   "type": "time",
        |   "value": 10,
        |   "unit": "m"
        | },
        | "policy":{
        |   "type":"scale",
        |   "unit":"number",
        |   "value":2,
        |   "first_alert": 1
        | }
        |}
      """.stripMargin


    RedisOperations.addTableCache(key, field, value3)


  }


  test("cache") {
    ConfigUtils.configBuilder(Map(
      appName -> "RedisOperationsSuite",
      master -> "local[2]",
      "spark.redis.host" -> "127.0.0.1",
      "spark.redis.port" -> "6379",
      "spark.redis.db" -> "4"
    ))

    val spark = SparkRuntime.getSparkSession

    val key = "sqlalarm_cache:uuid00000001:sqlalarm_job_001:Fail"

    val json = JacksonUtils.prettyPrint[RecordDetail](RecordDetail(
      "jobid",
      "fail",
      "2019",
      "sss",
      "cont",
      "title",
      "ppp",
      "001",
      "sss",
      "tt",
      1
    ))

    val rdd = spark.sparkContext.parallelize(Seq(json), 1)


    RedisOperations.setListCache(key, rdd, SaveMode.Overwrite)


  }

  test("ops") {
    ConfigUtils.configBuilder(Map(
      appName -> "RedisOperationsSuite",
      master -> "local[2]",
      "spark.redis.host" -> "127.0.0.1",
      "spark.redis.port" -> "6379",
      "spark.redis.db" -> "4"
    ))
    val spark = SparkRuntime.getSparkSession
    val rdd = RedisOperations.getListCache("test:111*")

    val map = RedisOperations.getTableCache("sqlalarm_policy*")
    val conf = map.collect().toMap


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
