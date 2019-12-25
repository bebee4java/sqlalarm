package dt.sql.alarm.conf

import dt.sql.alarm.input.Constants.SubscribeType.SubscribeType

/**
  *
  * Created by songgr on 2019/12/25.
  */
case class KafkaConf(
      subscribeType:SubscribeType,
      topic:String,
      servers:String,
      group:String) extends Conf
