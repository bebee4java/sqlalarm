package dt.sql.alarm.test

import dt.sql.alarm.input.KafkaInput
import org.scalatest.FunSuite


class InputSuite extends FunSuite with LocalSparkApp {

  test("kafka input test") {
    val session = spark
    val ds = new KafkaInput().getDataSetStream(session)
    assert(ds != null)
  }

}
