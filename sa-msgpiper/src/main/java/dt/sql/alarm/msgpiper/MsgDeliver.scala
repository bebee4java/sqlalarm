package dt.sql.alarm.msgpiper

import dt.sql.alarm.log.Logging
import dt.sql.alarm.msgpiper.constants.Constants
import dt.sql.alarm.utils.ConfigUtils
import java.util

import org.apache.commons.lang3.StringUtils
import redis.clients.jedis.{Jedis, JedisCluster, JedisCommands}
import scala.collection.JavaConverters._

/**
  * abstract message deliver redis class
  * Created by songgr on 2019/12/19.
  */

abstract class MsgDeliver extends Serializable with Logging {
  protected def clinet : JedisCommands

  /**
    * 销毁redis连接池
    */
  def destroy

  /**
    * 获得缓存key列表 (不建议使用！！)
    * @param pattern 匹配模式
    * @return Set[String]
    */
  @deprecated(message = "this command is not recommended")
  def getKeys(pattern: String): Set[String]

  /**
    * 关闭redis单个client
    * @param _clinet redis客户端
    */
  protected def close(_clinet: JedisCommands): Unit = {
    try {
      if (_clinet == null) return
      _clinet match {
        case j:Jedis => j.asInstanceOf[Jedis].close()
        case j:JedisCluster => j.asInstanceOf[JedisCluster].close()
        case _ =>
      }
    } catch {
      case e: Exception =>
        logError("returnResource error.", e)
    }
  }

  /**
    * 往队列发送消息
    * @param toQueue 队列名称
    * @param msgStr 消息
    * @param expireSec 过期时间，单位秒
    * @return int
    */
  def sendMsg(toQueue: String, msgStr: String, expireSec: Int): Int = {
    var jedis: JedisCommands = null
    try {
      val MAX_MSG_LEN = MsgDeliver.conf.getOrElse(Constants.MSG_PIPER_MAXLENGTH, 1000000).asInstanceOf[Int]
      if (StringUtils.isNoneBlank(msgStr) && msgStr.length > MAX_MSG_LEN) {
        logWarning("msg is to long to send!! msg length = " + msgStr.length)
        Constants.MSG_TOO_LONG
      } else {
        jedis = clinet
        jedis.rpush(toQueue, msgStr)
        // 设置过期时间
        if (expireSec > 0)
          jedis.expire(toQueue, expireSec)
        Constants.SUCC_CODE
      }
    } catch {
      case e: Exception => {
        logError(s"sed msg error! toQueue: $toQueue , msgStr: $msgStr", e)
        Constants.FAIL_CODE
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 往队列发送消息
    * @param toQueue 队列名称
    * @param msgStr 消息
    * @return int
    */
  def sendMsg(toQueue: String, msgStr: String): Int = {
    sendMsg(toQueue, msgStr, -1)
  }

  /**
    * 从队列接收一条消息
    * @param fromQueue 队列名称
    * @return String
    */
  def receiveMsg(fromQueue: String): String = {
    var jedis: JedisCommands = null
    try {
      jedis = clinet
      jedis.lpop(fromQueue)
    } catch {
      case e:Exception => {
        logError(s"receive msg from queue $fromQueue error! " ,e)
        null
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 监听队列返回消息
    * @param fromQueue 队列名称
    * @param timeOut 超时时间，单位秒
    * @return String
    */
  def receiveMsg(fromQueue: String, timeOut: Int): String = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      val timeout = if (timeOut < 0) 0 else timeOut
      val list:util.List[String] = jedis.blpop(timeout, fromQueue)
      if (list != null && list.size() >1)
        return list.get(1)
    } catch {
      case e:Exception => {
        logError(s"receive msg from queue $fromQueue error! " ,e)
      }
    } finally {
      close(jedis)
    }
    null
  }

  /**
    * 获取缓存数据
    * @param key 缓存key
    * @return String
    */
  def getCache(key: String): String = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      jedis.get(key)
    } catch {
      case e:Exception => {
        logError(s"get cache error from key: $key" ,e)
        null
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 设置缓存数据
    * @param key 缓存key
    * @param value 数据内容
    * @return Int
    */
  def setCache(key: String, value: String): Int = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      jedis.set(key, value)
      Constants.SUCC_CODE
    } catch {
      case e:Exception => {
        logError(s"get cache error from key: $key" ,e)
        Constants.FAIL_CODE
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 删除缓存
    * @param key 缓存key
    * @return Int
    */
  def delCache(key: String): Int = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      jedis.del(key)
      Constants.SUCC_CODE
    } catch {
      case e:Exception => {
        logError(s"del cache error key: $key" ,e)
        Constants.FAIL_CODE
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 获取table所有数据
    * @param key table key
    * @return Map[String, String]
    */
  def getTableCache(key: String): Map[String, String] = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      jedis.hgetAll(key).asScala.toMap[String,String]
    } catch {
      case e:Exception => {
        logError(s"get table cache error key: $key" ,e)
        null
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 获取table一条数据
    * @param key table key
    * @param field table field
    * @return String
    */
  def getTableCache(key: String, field: String): String = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      jedis.hget(key, field)
    } catch {
      case e:Exception => {
        logError(s"get table cache error key: $key , field: $field" ,e)
        null
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 添加table单条缓存
    * @param key table key
    * @param field table field
    * @param value 数据内容
    * @return Int
    */
  def addTableCache(key: String, field: String, value: String): Int = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      jedis.hset(key, field, value)
      Constants.SUCC_CODE
    } catch {
      case e:Exception => {
        logError(s"add table cache error key: $key , field: $field , value: $value" ,e)
        Constants.FAIL_CODE
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 添加table多条缓存
    * @param key table key
    * @param value table 数据
    * @return Int
    */
  def addTableCache(key: String, value: Map[String, String]): Int = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      import scala.collection.JavaConversions._
      jedis.hmset(key, value)
      Constants.SUCC_CODE
    } catch {
      case e:Exception => {
        logError(s"add table cache error key: $key , value: $value" ,e)
        Constants.FAIL_CODE
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 获取table所有key列表
    * @param key table key
    * @return Set[String]
    */
  def getTableKeys(key: String): Set[String] = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      jedis.hkeys(key).asScala.toSet[String]
    } catch {
      case e:Exception => {
        logError(s"get table keys error key: $key" ,e)
        null
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 判断key是否存在
    * @param key 缓存key
    * @return Boolean
    */
  def exists(key: String): Boolean = {
    var jedis:JedisCommands = null
    try {
      if (key != null && !key.isEmpty) {
        jedis = clinet
        return jedis.exists(key)
      }
    } catch {
      case e:Exception => {
        logError(e.getMessage, e)
      }
    } finally {
      close(jedis)
    }
    false
  }

  /**
    * 获取队列某个值
    * @param fromQueue 队列名称
    * @param index 下标
    * @return String
    */
  def getListElement(fromQueue: String, index: Long): String = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      jedis.lindex(fromQueue, index)
    } catch {
      case e:Exception => {
        logError(s"get an element from a list by its index error! index: $index" ,e)
        null
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 获取队列范围数据
    * @param fromQueue 队列名称
    * @param start 下标开始
    * @param stop 下标结束
    * @return List[String]
    */
  def getListRange(fromQueue: String, start:Long, stop:Long): List[String] = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      jedis.lrange(fromQueue, start, stop).asScala.toList
    } catch {
      case e:Exception => {
        logError(s"get a range of elements from a list error! start: $start, stop: $stop" ,e)
        null
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 获取队列长度
    * @param fromQueue 队列名称
    * @return Long
    */
  def getListLen(fromQueue: String):Long = {
    var jedis:JedisCommands = null
    try {
      jedis = clinet
      jedis.llen(fromQueue)
    } catch {
      case e:Exception => {
        logError(s"get list length error! queue: $fromQueue" ,e)
        0L
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 获取Set缓存
    * @param key set的key
    * @return Set[String]
    */
  def getSetCache(key: String): Set[String] = {
    var jedis: JedisCommands = null
    try {
      jedis = clinet
      jedis.smembers(key).asScala.toSet[String]
    } catch {
      case e: Exception => {
        logError(s"get set cache error! key: $key", e)
        Set.empty[String]
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 添加set cache 成员
    * @param key    set的key
    * @param values 需要添加的values
    * @return Int
    */
  def addSetCache(key: String, values: String*): Int = {
    var jedis: JedisCommands = null
    try {
      jedis = clinet
      jedis.sadd(key, values: _*)
      Constants.SUCC_CODE
    } catch {
      case e: Exception => {
        logError(s"add set cache error! key: $key ,values: $values", e)
        Constants.FAIL_CODE
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 删除set cache成员
    * @param key    set的key
    * @param values 需要删除的values
    * @return Int
    */
  def dropSetCache(key: String, values: String*): Int = {
    var jedis: JedisCommands = null
    try {
      jedis = clinet
      jedis.srem(key, values: _*)
      Constants.SUCC_CODE
    } catch {
      case e: Exception => {
        logError(s"drop set cache error! key: $key ,values: $values", e)
        Constants.FAIL_CODE
      }
    } finally {
      close(jedis)
    }
  }

  /**
    * 判断value是否为set cache成员
    *
    * @param key   set的key
    * @param value 需要判断的value
    * @return Boolean
    */
  def isSetCacheMeb(key: String, value: String): Boolean = {
    var jedis: JedisCommands = null
    try {
      jedis = clinet
      jedis.sismember(key, value)
    } catch {
      case e: Exception => {
        logError(s"drop set cache error! key: $key ,values: $value", e)
        false
      }
    } finally {
      close(jedis)
    }
  }

}

object MsgDeliver extends Logging{
  private var msgDeliver :MsgDeliver = null
  private var conf:Map[String,Object] = null

  def getInstance:MsgDeliver = {
    if (msgDeliver == null ){
      this.synchronized {
        if (msgDeliver == null) {
          val map = ConfigUtils.getAnyValue(Constants.MSG_DELIVER).asInstanceOf[util.HashMap[String,Object]]
          conf = map.asScala.toMap
          val clazz = map.getOrDefault(Constants.MSG_PIPER_CLASS, Constants.MSG_PIPER_DEFAULT_CLASS)
          logInfo("init MsgDeliver by class: " + clazz)

          msgDeliver = Class.forName(clazz.asInstanceOf[String])
            .getConstructor(classOf[Map[String,Object]]).newInstance(conf)
            .asInstanceOf[MsgDeliver]
        }
      }
    }
    msgDeliver
  }
}
