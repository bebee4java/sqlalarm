package dt.sql.alarm.msgpiper

import dt.sql.alarm.log.Logging
import dt.sql.alarm.msgpiper.constants.Constants
import dt.sql.alarm.utils.ConfigUtils
import java.util

import org.apache.commons.lang3.StringUtils
import redis.clients.jedis.{Jedis, JedisCluster, JedisCommands}

/**
  * abstract message deliver redis class
  * Created by songgr on 2019/12/19.
  */

abstract class MsgDeliver extends Serializable with Logging {
  protected def clinet : JedisCommands

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

  def sendMsg(toQueue: String, msgStr: String): Int = {
    sendMsg(toQueue, msgStr, -1)
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
          import scala.collection.JavaConverters._
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
