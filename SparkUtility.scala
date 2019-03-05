package hcl.utils

import org.apache.spark.sql.SparkSession

class SparkUtility {
  def getSparkSession(): SparkSession = {

    val hiveLocation = "/tmp"
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SCD2_Scala")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", hiveLocation)
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    spark
  }
}
