package dt.sql.alarm.conf

/**
  *
  * Created by songgr on 2020/01/07.
  */
case class JdbcConf(
    url:String,                            // jdbc url
    driver:String,                         // jdbc 驱动类
    user:String,                           // jdbc 用户名
    password:String,                       // jdbc 密码
    var dbtable:String = "sqlalarm_records_log", // jdbc 表名
    var numPartitions:Int = 8,                 // 表写入可用于并行处理的最大分区数
    var batchsize:Int = 1000,               // JDBC批处理大小，它确定每次往返要插入多少行。这可以帮助提高JDBC驱动程序的性能
    var mode:String = "append"              // jdbc 表写入模式
) extends Conf
