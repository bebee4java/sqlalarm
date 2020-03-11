package dt.sql.alarm.reduce.engine

// 窗口
trait AggWindow
// 时间窗口
object TimeWindow extends AggWindow
// 时间+次数 窗口
object TimeCountWindow extends AggWindow
// 数量窗口
object NumberWindow extends AggWindow