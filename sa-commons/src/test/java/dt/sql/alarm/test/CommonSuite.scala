package dt.sql.alarm.test

import java.util
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.scalatest.FunSuite

class CommonSuite extends FunSuite {

  test("config properties") {

    val properties = new Properties()

    properties.setProperty("redis.msg.piper.class", "classA")
    properties.setProperty("redis.msg.maxlength", "100")
    properties.setProperty("redis.database", "4")

    val conf = ConfigFactory.parseProperties(properties)
    println(conf.getInt("redis.database"))

    assert(conf.getInt("redis.database") == 4)
  }

  test("config map") {

    val list = new util.ArrayList[String]()
    list.add("127.0.0.1:6379")

    val _map = Map[String,Object]("redis.msg.piper.class" -> "classA",
      "redis.msg.maxlength" -> "100",
      "redis.database" -> "4",
      "redis.addresses" -> list
    )
    import scala.collection.JavaConversions._
    val conf = ConfigFactory.parseMap(_map)

    println(conf.getInt("redis.database"))

    val entry = conf.entrySet()

    val keys = entry.map(map => map.getKey)
    println(keys)


    assert(conf.getInt("redis.database") == 4)

  }

}
