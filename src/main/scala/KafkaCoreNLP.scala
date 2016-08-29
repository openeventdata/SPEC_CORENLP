/**
 * Created by root on 2/4/16.
 */

import com.google.gson.Gson
import com.mongodb.hadoop.MongoOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.bson.BSONObject

object KafkaCoreNLP {
  def main(args: Array[String]) {
    /*if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }*/


    val Array(zkQuorum, group, topics, numThreads) = Array("dmlhdpc1", "dmlhdpc1", "test", "20")

    val sparkHome = "/usr/local/spark"
    val sparkMasterUrl = "spark://dmlhdpc10:7077"
    //val sparkMasterUrl = "spark://Latifurs-MacBook-Pro.local:7077"



    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setMaster(sparkMasterUrl)

      .setSparkHome(sparkHome)
      .setAppName("KafkaCoreNLP")
      .set("spark.cores.max", "40")
      .setJars(Seq(System.getProperty("user.dir") + "/target/scala-2.10/TestSpark-assembly-1.0.jar"))
        .set("spark.akka.heartbeat.interval", "100")


    val ssc = new StreamingContext(sparkConf, Seconds(2 * 60))
    ssc.checkpoint("checkpoint")

    //ssc.sparkContext.addJar("")


    //props.put(ProducerConfig.)





    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)


    val kafka_producer = SharedSingleton{
      val props = new java.util.HashMap[String, Object]()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dmlhdpc1:9092")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](props)
      producer
    }

    val gson = SharedSingleton{
      new Gson()
    }


    try{
      val shift_Reduce = true
      val result = lines.filter(x => !x.trim.isEmpty).map{
        x =>
          print(x)
          // For English
          val obj = ParseAndDumpToMongoDB.ExtractsDocInfo(x, shift_Reduce, "ENG")

          val messageStr = gson.get.toJson(obj)

          val message = new ProducerRecord[String, String]("petrarch", null, messageStr)
          kafka_producer.get.send(message)
          (null , obj)

        /* val message = new ProducerRecord[String, String](topic, null, str)

        // For Arabic
        val parser = new ArabicParserForMongoDB()
        if (shift_Reduce)
          {
            val obj = parser.parseArabicDoc(x, shift_Reduce,
              "/opt/custom_lib/models/arabicSR.ser.gz",
              "/opt/custom_lib/tagger/arabic.tagger");
            (null , obj)
          }

        else
          {
            val obj = parser.parseArabicDoc(x, shift_Reduce,
              "/opt/custom_lib/models/arabicFactored.ser.gz",
              "/opt/custom_lib/tagger/arabic.tagger");
            (null , obj)
          }*/

      }.filter(x => x._2 != null)

      result.print()

      result.foreachRDD{
        x =>
          val outputConfig = new Configuration()
          outputConfig.set("mongo.output.uri",
            "mongodb://dmlhdpc10:27017/output_real.collection")

          x.saveAsNewAPIHadoopFile(
            "file:///this-is-completely-unused",
            classOf[Object],
            classOf[BSONObject],
            classOf[MongoOutputFormat[Object, BSONObject]],
            outputConfig)
      }
    }

    catch {
      case e:Exception => e.printStackTrace()
    }

    ssc.start()
    ssc.awaitTermination()
  }



}
