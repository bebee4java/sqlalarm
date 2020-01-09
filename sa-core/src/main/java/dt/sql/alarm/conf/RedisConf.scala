package dt.sql.alarm.conf

/**
  *
  * Created by songgr on 2020/01/09.
  */
case class RedisConf(
      keys:String,                 // redis流的key值,多个逗号分隔
      start_offsets:String,              // redis流的起始offset
      group:String,                // redis流消费组
      consumer_prefix:String,      // redis流消费者前缀
      var parallelism:Int = 4,         // redis流处理并行度
      var batch_size:Int = 200,        // redis流批次数据量大小
      var read_block_msec:Long = 1000L // redis流批次等待时间毫秒值
) extends Conf

