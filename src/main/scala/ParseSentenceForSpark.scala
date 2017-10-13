import com.mongodb.hadoop.MongoOutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject

/**
 * Created by root on 10/29/15.
 */
object ParseSentenceForSpark {
  def main(args: Array[String]) {
    val sparkHome = "/usr/local/spark"
    val sparkMasterUrl = "spark://dmlhdpc10:7077"


    val conf = new SparkConf()
      .setMaster("local[*]")
      .setSparkHome(sparkHome)
      .setAppName("Spark Core Nlp Test")
      //.set("spark.rdd.compress", "true")
      //.set("spark.storage.memoryFraction", "1")
      //.set("spark.core.connection.ack.wait.timeout", "600")
      //.set("spark.akka.frameSize", "50")




    val sc = new SparkContext(conf)
    //sc.addJar("hdfs://dmlhdpc10:9000/core_nlp_lib/test_corenlp-assembly-1.0.jar")


    //English
    //val p_sentences = sc.textFile("hdfs://dmlhdpc10:9000/ENG/APW/*.xml", 250)
    val p_sentences = sc.textFile("hdfs://dmlhdpc10:9000/ENG/APW/apw_eng_200812_hadoop.xml", 200)

    //Spanish
    //val p_sentences = sc.textFile("hdfs://dmlhdpc10:9000/ESP/AFP/afp_spa_19941*.xml", 200)
    //val p_sentences = sc.textFile("hdfs://dmlhdpc10:9000/ESP/XIN/*.xml")


    //Arabic
    //val p_sentences = sc.textFile("hdfs://dmlhdpc10:9000/ENG/APW/*.xml", 250)
    //val p_sentences = sc.textFile("hdfs://dmlhdpc10:9000/ARB/AAW/aaw_arb_20061*.xml", 200)

    //p_sentences.collect().foreach(println)
    //val p_sentences = sc.parallelize(sentences)

    try{
      val shift_Reduce = true
      val result = p_sentences.filter(x => !x.trim.isEmpty).map{
        x =>
          // For English or Spanish
          val obj = ParseAndDumpToMongoDB.ExtractsDocInfo(x, shift_Reduce, "ENG")
          (null , obj)

          /*
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

      val outputConfig = new Configuration()
      outputConfig.set("mongo.output.uri",
        "mongodb://dmlhdpc10:27017/output1.collection")

      result.saveAsNewAPIHadoopFile(
        "file:///this-is-completely-unused",
        classOf[Object],
        classOf[BSONObject],
        classOf[MongoOutputFormat[Object, BSONObject]],
        outputConfig)
    }

    catch {
      case e:Exception => e.printStackTrace()
    }


  }

}
