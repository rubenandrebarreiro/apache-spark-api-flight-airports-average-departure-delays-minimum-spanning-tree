package cadlabs

import java.util
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.Datase
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object WordCount {

  private val DefaulftFile = "data/hamlet.txt"

  def wordCount(data: Nothing): Nothing = {

    val words = data.flatMap((s) => Arrays.asList(s.toLowerCase.split(" ")).iterator.asInstanceOf[Nothing], Encoders.STRING)

    words.printSchema

    val result = words.groupBy("value").count

    result.printSchema
    result.sort(functions.desc("count"))
  }

  def main(args: Array[Nothing]): Unit = {

    val file =
      if (args.length < 1)
        DefaulftFile
      else
        args(0)

    // Start Spark session (SparkContext API may also be used)
    // Master("local") indicates local execution
    val spark = SparkSession.builder.appName("WordCount").master("local").getOrCreate

    // Only error messages are logged from this point onward
    // Comment (or change configuration) if you want the entire log
    spark.sparkContext.setLogLevel("ERROR")
    val textFile = spark.read.textFile(file).as(Encoders.STRING)
    val counts = wordCount(textFile)

    // Print all as list
    //		System.out.println(counts.collectAsList());
    var i = 1

    for (r <- counts.take(20).asInstanceOf[Array[Nothing]]) {
      System.out.println({
        i += 1; i - 1
      } + ": " + r)
    }

    spark.stop
  }
}