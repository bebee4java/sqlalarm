package dt.sql.alarm.test

import org.apache.spark.sql.SparkSession

trait LocalSparkApp {

  def spark = {
    SparkSession.builder()
      .appName("LocalSparkApp")
      .master("local[*]")
      .getOrCreate()
  }

}
