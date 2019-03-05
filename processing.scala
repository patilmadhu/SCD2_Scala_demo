package hcl.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import hcl.utils.LookupMaps

class processing(spark:SparkSession) extends LookupMaps {

      def poc2_processing():Unit={

            import spark.implicits._
            val pcAccountOrigFile = spark.read.option("inferSchema","true").csv("/user/dbuser11/curation/pc_account_orig.csv")
            val pcAccountOrigDF = pcAccountOrigFile.select(pcAccountOrigFile.columns.map(c => col(c).as(pc_account_orig_map.getOrElse(c, c))): _*)

            val pcAccountModFile = spark.read.option("inferSchema","true").csv("/user/dbuser11/curation/pc_account_mod.csv")
            val pcAccountModDF = pcAccountModFile.select(pcAccountModFile.columns.map(c => col(c).as(pc_account_mod_map.getOrElse(c, c))): _*)
              .withColumn("OriginationDate", lit(null))


            val totalDF = pcAccountOrigDF.union(pcAccountModDF)
            //totalDF.write.saveAsTable("poc3dbuser11.pc_account")

            val validationDF = totalDF
              .withColumn("validFlag", when($"PublicID".startsWith("pc:")
                && lower($"ServiceTier").isin("bronze","gold","silver","platinum") ,lit("valid")).otherwise(lit("invalid")))

            validationDF.show(false)
            validationDF.filter($"validFlag" === "valid").show(false)
            validationDF.filter($"validFlag" === "valid").drop("validFlag").write.mode("overwrite").saveAsTable("poc3dbuser11.valid_pc_account")

            validationDF.filter($"validFlag" === "invalid").show(false)
            validationDF.filter($"validFlag" === "invalid").drop("validFlag").write.mode("overwrite").saveAsTable("poc3dbuser11.invalid_pc_account")



          }


        }
