package com.redislabs.provider.redis

import java.util.concurrent.ConcurrentHashMap

import dt.sql.alarm.core.Constants._
import org.apache.spark.SparkEnv
import redis.clients.jedis._
import redis.clients.jedis.exceptions.JedisConnectionException

import scala.collection.JavaConversions._


object ConnectionPool {
  @transient private lazy val pools: ConcurrentHashMap[RedisEndpoint, JedisPoolAbstract] =
    new ConcurrentHashMap[RedisEndpoint, JedisPoolAbstract]()

  private lazy val sparkConf = SparkEnv.get.conf

  def connect(re: RedisEndpoint): Jedis = {
    val pool = pools.getOrElseUpdate(re,
      {
        val poolConfig: JedisPoolConfig = new JedisPoolConfig()
        poolConfig.setMaxTotal(250)
        poolConfig.setMaxIdle(32)
        poolConfig.setTestOnBorrow(false)
        poolConfig.setTestOnReturn(false)
        poolConfig.setTestWhileIdle(false)
        poolConfig.setMinEvictableIdleTimeMillis(60000)
        poolConfig.setTimeBetweenEvictionRunsMillis(30000)
        poolConfig.setNumTestsPerEvictionRun(-1)
        poolConfig.setMaxWaitMillis(10000)

        if (SPARK_REDIS_SENTINEL_MODE.equalsIgnoreCase(sparkConf.get(SPARK_REDIS_MODE, SPARK_REDIS_SENTINEL_MODE))){
          // 哨兵模式
          val master = sparkConf.get(SPARK_REDIS_MASTER, SPARK_REDIS_MASTER_DEFAULT)
          val sentinels = new java.util.HashSet[String]()
          re.host.split(",").filter(_.nonEmpty).foreach(add => sentinels.add(add))

          new JedisSentinelPool(master, sentinels, poolConfig, re.timeout, re.auth, re.dbNum)
        } else {
          new JedisPool(poolConfig, re.host, re.port, re.timeout, re.auth, re.dbNum)
        }
      }
    )
    var sleepTime: Int = 4
    var conn: Jedis = null
    while (conn == null) {
      try {
        conn = pool.getResource
      }
      catch {
        case e: JedisConnectionException if e.getCause.toString.
          contains("ERR max number of clients reached") => {
          if (sleepTime < 500) sleepTime *= 2
          Thread.sleep(sleepTime)
        }
        case e: Exception => throw e
      }
    }
    conn
  }
}
