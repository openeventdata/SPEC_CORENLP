/**
 * Created by root on 2/4/16.
 */

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

object DirectKafkaCoreNLP {
  def main(args: Array[String]) {
    /*if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaCoreNLP <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }*/

    //args[0] = "10.176.148.51"
    //args[1] = "test"
    val Array(brokers, topics) = Array("dmlhdpc1:9092", "test")

    val sparkHome = "/usr/local/spark"
    val sparkMasterUrl = "spark://dmlhdpc10:7077"


    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setSparkHome(sparkHome)
      .setAppName("DirectKafkaCoreNLP")

    // Create context with 2 second batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
