//import com.cloudera.datascience.common.XmlInputFormat

import com.mongodb.hadoop.MongoOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BasicBSONObject, BSONObject}

/**
 * Created by root on 10/28/15.
 */
object TestMain {

//  def readFile(path: String, start_tag: String, end_tag: String, sc: SparkContext) = {
//    val conf = new Configuration()
//    conf.set(XmlInputFormat.START_TAG_KEY, start_tag)
//    conf.set(XmlInputFormat.END_TAG_KEY, end_tag)
//    val rawXmls = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable],
//      classOf[Text], conf)
//    rawXmls.map(p => p._2.toString)
//  }

  def extractField(tuple: String, tag: String) = {
    var value = tuple.replaceAll("\n", " ").replace("<\\", "</")

    if (value.contains("<" + tag + ">") && value.contains("</" + tag + ">")) {

      value = value.split("<" + tag + ">")(1).split("</" + tag + ">")(0)

    }
    value
  }


  def main (args: Array[String]) {

    val sparkHome = "/usr/local/spark"
    val sparkMasterUrl = "spark://dmlhdpc10:7077"


    val conf = new SparkConf()
      .setMaster("local")
      .setSparkHome(sparkHome).setAppName("Sigmoid Spark")
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.set("spark.executor.memory", "1g")
    //.set("spark.rdd.compress", "true")
    //.set("spark.storage.memoryFraction", "1")

    val sc = new SparkContext(conf)

    //val pages = readFile("hdfs://dmlhdpc10:9000/input_data.xml", "<user>", "<\\user>", sc)

    //println(pages.collect())

    //    val xmlUserDF = pages.map { tuple =>
    //      val account = extractField(tuple, "account")
    //      val name = extractField(tuple, "name")
    //      val number = extractField(tuple, "number")
    //
    //      (account, name, number)
    //    }
    //
    //    println(xmlUserDF.count())

    //val data = sc.parallelize(1 to 99999).collect().filter( _ < 10000)
    //data.foreach(println)

    /*val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class",
      "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<users")
    jobConf.set("stream.recordreader.end", "</users>")
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf,
      "hdfs://dmlhdpc10:9000/input_data.xml")

    val documents = sc.hadoopRDD(jobConf,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text]).collect()

    //documents.foreach(x => println(x))

    //val doc = sc.textFile("hdfs://dmlhdpc10:9000/input_data.xml").collect()
    //doc.foreach(x => println(x))


    val texts = documents.map(_._1.toString)
      .map{ s =>
        val xml = XML.loadString(s)
        val id = (xml \ "account").text
        val title = (xml \ "name").text
        val number = (xml \ "number").text.toInt
        (id, title, number )
      }.toList

    texts.foreach(println)

    */


    //sc.addJar("hdfs://dmlhdpc10:9000/core_nlp_lib/test_corenlp-assembly-1.0.jar")

    val sentences = List("I am rakib", "He is Ahsan")

    val p_sentences = sc.parallelize(sentences)

    /*p_sentences.map(x => ParseSentence.parseAndStoreSentenceToMongoDB(x)).collect()
    .foreach(println)

    */

    val counts = p_sentences.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    val result = counts.map{
      x =>
        val bson = new BasicBSONObject()
        bson.put("word", x._1)
        bson.put("count", x._2.toString)
        (null, bson)
    }

    val outputConfig = new Configuration()
    outputConfig.set("mongo.output.uri",
      "mongodb://dmlhdpc10:27017/output.collection")

    result.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[BSONObject],
      classOf[MongoOutputFormat[Object, BSONObject]],
      outputConfig)
  }

}
