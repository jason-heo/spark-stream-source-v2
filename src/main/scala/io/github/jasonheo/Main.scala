package io.github.jasonheo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("io.github.jasonheo.sources").setLevel(Level.INFO)

    val spark = SparkSession.builder().master("local[4]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val df = spark
      .readStream
      .format("io.github.jasonheo.sources.RandomIntStreamProvider")
      .option("numPartitions", "1") // partition 개수, Task로 할당된다. executor 개수가 넉넉한 경우 읽기 병렬성은 높일 수 있다
      .load()

    df.printSchema()

    import scala.concurrent.duration._

    val query: StreamingQuery = df
      .writeStream
      .format("console")
      .trigger(org.apache.spark.sql.streaming.Trigger.Continuous(2.seconds))
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
