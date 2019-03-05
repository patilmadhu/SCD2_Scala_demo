package hcl
import org.apache.log4j.{Level, Logger}
import hcl.processing._
import hcl.utils._

object MainClass extends Serializable
{
  def main(args: Array[String]): Unit = {
   Logger.getLogger("org").setLevel(Level.WARN)
   Logger.getLogger("akka").setLevel(Level.WARN)
   val sparkUtility = new SparkUtility
   val spark = sparkUtility.getSparkSession()

    val x = new processing(spark)
    x.poc2_processing()

      }
}
