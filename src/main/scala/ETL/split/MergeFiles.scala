package ETL.split
import LoadAndSplitFiles.spark

object MergeFiles extends App {
  val readFiles = spark.read.option("header", "true").option("inferSchema", "true")
    .csv("/home/knoldus/Desktop/spark-ETL/src/main/resources/splitted-files")


  readFiles.coalesce(1).write.mode("overwrite").option("header", "true")
    .format("csv").save("/home/knoldus/Desktop/spark-ETL/src/main/resources/mergedFile")

}
