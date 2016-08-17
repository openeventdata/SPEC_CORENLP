import java.io.File

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject

import scala.xml.XML._

/**
 * Created by root on 10/22/15.
 */
object ParseEnglishCorpa {

  //val dtd_file = "/home/rakib/data/NLP/dataset/gigaworld/shared/mlrdir3/disk1/mlr/corpora/LDC2009T13/dtd/gigaword.dtd"
  //val dtd_file_content = scala.io.Source.fromFile(dtd_file).getLines().mkString + "\n"

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getRecursiveListOfFiles(dir: File): Array[File] = {
    val these = dir.listFiles
    these ++ these.filter(_.isDirectory).flatMap(getRecursiveListOfFiles)
  }

  def parseFile(file: File)= {
    if (!file.isDirectory) {
      print("File name: " + file.getAbsoluteFile + "\n")
      parseXML_(scala.io.Source.fromFile(file).getLines().mkString("\n"))
    }

  }

  def parseXML(xml_String: String) {

    //val xml_dtd_tag = "<!DOCTYPE GWENG SYSTEM \"" + dtd_file + "\">"

    val xml = loadString(xml_String)


    val jsonObject = (xml \\ "DOC").map{
      doc =>

        val mongoClient = MongoClient("10.176.148.60", 27017)
        val db = mongoClient("nlp")
        val coll = db("nlp_text")

        val text = (doc \\ "TEXT")
        var sentence_id = 0;
        val parse_sentence = (text \\ "P").map{ sentence =>
          sentence_id += 1
          val trim_sentence = sentence.text.trim().replace("\n", " ")
          try{

            var parseTree = ""
            var ner = ""
            var pen_parse_tree = ""
            if (trim_sentence.trim() != ""){
                parseTree = ParseSentence.getPennParseTree(trim_sentence)
                ner = ParseSentence.getNER(trim_sentence)
            }

            MongoDBObject("sentence_id" -> sentence_id,
              "sentence" -> trim_sentence,
              "parse_sentence" -> parseTree,
              "ner" -> ner)

          } catch{
            case e:Exception => print(e.getMessage)
          }
        }

        //coll.remove(MongoDBObject.empty)

        val builder = MongoDBObject.newBuilder
        builder += "type" -> ((doc \\ "@TYPE").text.trim())

        val doc_id = (doc \\ "@ID").text.trim()
        builder += "doc_id" -> doc_id
        builder += "head_line" -> (doc \\ "HEADLINE").text.trim()
        builder += "date_line" -> (doc \\ "DATELINE").text.trim()
        builder += "sentences" -> parse_sentence

        val result = builder.result()
        val query = MongoDBObject("doc_id" -> doc_id)
        val doc_ = coll.findAndModify(query = query,
          update = result,
          upsert = true,
          fields = null,
          sort = null,
          remove = false,
          returnNew = true)


        //val alldoc = coll.find(query)
        //for(doc <- alldoc) println(doc)
        mongoClient.close()

    } // Map of json Object ends here



  }


  def parseXML_(xml_String: String) {

    //val xml_dtd_tag = "<!DOCTYPE GWENG SYSTEM \"" + dtd_file + "\">"

    val xml = loadString(xml_String)


    val jsonObject = (xml \\ "DOC").map{
      doc =>

        ParseAndDumpToMongoDB.parseEnglishDOC(doc.toString())

    } // Map of json Object ends here
  }

  def parseXML_Test(xml_String: String): String = {

    //val xml_dtd_tag = "<!DOCTYPE GWENG SYSTEM \"" + dtd_file + "\">"

    val xml = loadString(xml_String)

    (xml \\ "DOC").map(x => ParseAndDumpToMongoDB.parseEnglishDOC_(x.toString())).mkString

    // Map of json Object ends here
  }

  def main(args: Array[String]) {

    //val dir = "/home/rakib/data/NLP/dataset/gigaworld/shared/mlrdir3/disk1/mlr/corpora/LDC2009T13/data/";

    val start_time = System.currentTimeMillis()

    val file = "/opt/TestSpark/target/scala-2.10/test.xml"

    val lines = scala.io.Source.fromFile(file).getLines().toList

    lines.filter(x => !x.trim.isEmpty).map(x => ParseEnglishCorpa.parseXML_(x))





    val timeInHour = (System.currentTimeMillis() - start_time) / (1000 * 60 * 60)
    print("Total time (Hour): " + timeInHour)
  }

}
