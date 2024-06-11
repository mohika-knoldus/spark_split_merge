package ETL.split

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LoadAndSplitFiles {
val spark = SparkSession.builder()
  .appName("Spark- ETL")
  .master("local[3]")
  .getOrCreate()

  val df = spark.read.option("inferSchema", "true").option("header", "true").option("mode", "DROPMALFORMED")
    .csv("/home/knoldus/Desktop/spark-ETL/src/main/resources/all_data.csv")

  val cleanedDf = df.na.drop("any")

  val dateFormatDf = cleanedDf.withColumn("date", date_format(to_date(col("Order date"), "MM/dd/yy HH:mm"), "dd-MM-yy"))

  val repartitionedDf = dateFormatDf.repartition(100, col("date")).drop("date")

  repartitionedDf.write.format("csv").mode("overwrite").option("header", "true")
    .save("/home/knoldus/Desktop/spark-ETL/src/main/resources/splitted-files")

//  cleanedDf.select(countDistinct("Product")).show(false)
//
//  cleanedDf.coalesce(100).write.partitionBy("Product")
//    .format("csv").save("/home/knoldus/Desktop/spark-ETL/src/main/resources/splittedFiles")

//  val repartitionedDf = cleanedDf.repartition(1000, col("Product"))
//
//  val coalescedDf = repartitionedDf.coalesce(100)
//
//  coalescedDf.write.format("csv").option("header", "true").mode("overwrite")
//    .save("/home/knoldus/Desktop/spark-ETL/src/main/resources/splitted-files")
}
