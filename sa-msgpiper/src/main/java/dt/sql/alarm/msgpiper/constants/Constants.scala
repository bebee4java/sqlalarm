package dt.sql.alarm.msgpiper.constants

object Constants {

  val MSG_DELIVER = "msg-deliver"
  val MSG_PIPER_CLASS = "msg.piper.class"
  val JEDIS_MINIDLE = "minidle"
  val JEDIS_MAXIDLE = "maxidle"
  val JEDIS_MAXTOTAL = "maxtotal"
  val JEDIS_PASSWORD = "password"
  val JEDIS_DATABASE = "database"
  val JEDIS_ADDRESSES = "addresses"
  val JEDIS_SENTINEL_MASTER = "sentinel.master"
  val MSG_PIPER_DEFAULT_CLASS = "dt.sql.alarm.msgpiper.single.JedisMsgDeliver"
  val MSG_PIPER_MAXLENGTH = "msg.piper.maxlength"

  /**
    * 成功码
    */
  val SUCC_CODE = 0

  /**
    * 发送失败码
    */
  val FAIL_CODE = -1

  /**
    * 消息超长码
    */
  val MSG_TOO_LONG = -2


}

