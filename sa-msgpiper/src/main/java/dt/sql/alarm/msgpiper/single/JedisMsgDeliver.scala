package dt.sql.alarm.msgpiper.single

import dt.sql.alarm.exception.SQLAlarmException
import dt.sql.alarm.msgpiper.MsgDeliver
import dt.sql.alarm.msgpiper.constants.Constants
import redis.clients.jedis.{Jedis, JedisCommands, JedisPool, JedisPoolConfig}

/**
  * single Jedis
  * Created by songgr on 2019/12/19.
  */
private[msgpiper] class JedisMsgDeliver(conf:Map[String,Object]) extends MsgDeliver {
  private val TIMEOUT = 10000
  @transient private var jedisPool: JedisPool = initPool

  override def clinet: JedisCommands = getJedis

  def initPool = {
    if (jedisPool != null) {
      logInfo("==== JedisMsgDeliver reconnect jedisPool... ====")
      jedisPool.close()
      jedisPool = null
    }
    logInfo("JedisPool init....")
    val minIdle = conf.getOrElse(Constants.JEDIS_MINIDLE, 5).asInstanceOf[Int]
    val maxIdle = conf.getOrElse(Constants.JEDIS_MAXIDLE, 10).asInstanceOf[Int]
    val maxTotal = conf.getOrElse(Constants.JEDIS_MAXTOTAL, 30).asInstanceOf[Int]
    val address = conf.get(Constants.JEDIS_ADDRESSES)
    val password = conf.getOrElse(Constants.JEDIS_PASSWORD, null).asInstanceOf[String]
    val dbIndex = conf.getOrElse(Constants.JEDIS_DATABASE, 0).asInstanceOf[Int]

    if (address == null || address.isEmpty || address.get.asInstanceOf[String].isEmpty) {
      throw new SQLAlarmException("redis address con't find!!!")
    }

    val _address = address.get.asInstanceOf[String].split(",")

    val config: JedisPoolConfig = new JedisPoolConfig()
    config.setMinIdle(minIdle)
    config.setMaxIdle(maxIdle)
    config.setMaxTotal(maxTotal)
    config.setTestWhileIdle(true)

    val strs = _address(0).split(":", -1)
    val host = strs(0)
    val port = Integer.parseInt(strs(1))

    val pool = new JedisPool(config, host, port, TIMEOUT, password, dbIndex)
    logInfo("JedisMsgDeliver create jedisPool succeed!")
    pool
  }

  override def destroy(): Unit = {
    if (jedisPool != null){
      jedisPool.destroy()
      jedisPool = null
    }
  }

  private def getJedis: Jedis = {
    if (jedisPool == null)
      jedisPool = initPool

    var jedis: Jedis = null
    jedis = jedisPool.getResource
    var retries = 0
    while (!jedis.isConnected || !(jedis.ping == "PONG")) {
      retries += 1
      close(jedis)

      if (retries >= 3)
        throw new SQLAlarmException("can not get resource from jedis pool...")

      jedis = jedisPool.getResource
    }
    jedis
  }

  override def getKeys(pattern: String): Set[String] = {
    var jedis:Jedis= null
    try {
      jedis = getJedis
      import scala.collection.JavaConverters._
      jedis.keys(pattern).asScala.toSet[String]
    } catch {
      case e:Exception => {
        logError(s"get keys error! pattern: $pattern" ,e)
        Set.empty[String]
      }
    } finally {
      close(jedis)
    }
  }
}
