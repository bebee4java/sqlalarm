package dt.sql.alarm.test

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.util.ConnectionUtils
import org.apache.spark.sql.SparkSession


object SparkRedisTest  {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkRedisTest")
      .master("local[4]")
      .config("spark.redis.host", "127.0.0.1")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    val sc = spark.sparkContext

    val keysRDD = sc.fromRedisKeyPattern()

    val stringRDD = sc.fromRedisKV(Array("test"))
    val strs = stringRDD.collect()

    val keys = keysRDD.collect()

    import spark.implicits._
    val df = stringRDD.toDF()


    println(keys.mkString(","))


    val listRDD = sc.fromRedisList(Array("list1"))

    val table = listRDD.toDF()
    table.printSchema()
    table.show()
    val tb = spark.read.json(listRDD.toDS())
    tb.printSchema()
    tb.show()

    val conn = RedisConfig.fromSparkConf(sc.getConf).initialHost.connect()

    val str = new RedisEndpoint(sc.getConf).connect().rpop("list1")

    println(str)

    val s = ConnectionUtils.withConnection[String](conn){
      conn =>
        conn.get("test")
    }

    println(s)

  }


}
