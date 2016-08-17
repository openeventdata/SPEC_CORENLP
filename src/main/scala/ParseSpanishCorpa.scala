import java.io.File

import scala.util.control.Breaks._
import scala.xml.XML._

/**
 * Created by root on 10/22/15.
 */
object ParseSpanishCorpa {

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


  def parseXML_(xml_String: String) {

    //val xml_dtd_tag = "<!DOCTYPE GWENG SYSTEM \"" + dtd_file + "\">"

    val xml = loadString(xml_String)


    val jsonObject = (xml \\ "DOC").map{
      doc =>

        ParseAndDumpToMongoDB.parseSpanishDOC(doc.toString())

    } // Map of json Object ends here
  }


  def main(args: Array[String]) {

    //val dir = "/home/rakib/data/NLP/dataset/gigaworld/shared/mlrdir3/disk1/mlr/corpora/LDC2009T13/data/";

    val start_time = System.currentTimeMillis()

    //val dir = "/home/rakib/data/NLP/dataset/gigaworld/shared/mlrdir3/disk1/mlr/corpora/LDC2009T21/data/afp_xml"
    val dir = "/home/rakib/data/NLP/dataset/gigaworld/shared/mlrdir3/disk1/mlr/corpora/LDC2009T21/dtd/xml"
    var count = 0;

    breakable {
      for (file <- getRecursiveListOfFiles(new File(dir))){
        if (!file.isDirectory)
        {
          try{
            parseFile(file)
          }
          catch {
            case e:Exception => print("Can not parse file: " + file.getName + "\n" + e.getMessage)
          }

          count += 1
        }

        if (count == 1)
          break
      }
    }

    val timeInHour = (System.currentTimeMillis() - start_time) / (1000 * 60 * 60)
    print("Total time (Hour): " + timeInHour)
  }

}
