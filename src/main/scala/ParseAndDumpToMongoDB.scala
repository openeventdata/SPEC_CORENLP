/**
 * Created by root on 10/8/15.
 */

import java.util.Properties

import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import edu.stanford.nlp.ling.CoreAnnotations.{SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation

import scala.collection.JavaConversions._
import scala.xml.XML._

object ParseAndDumpToMongoDB {

  def getPropertyForEnglish(enable_Shift_reduce_parser: Boolean): Properties = {

    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, cleanxml, pos, lemma, truecase, ner, parse, dcoref, sentiment")

    if (enable_Shift_reduce_parser)
    {
        props.put("parse.model", "edu/stanford/nlp/models/srparser/englishSR.beam.ser.gz")
    }

    //props.setProperty("regexner.mapping", "/usr/local/regexner.txt")
    props
  }

  def getPropertyForSpanish(enable_Shift_reduce_parser: Boolean): Properties = {

    val props = new Properties()
    props.setProperty("tokenize.language", "es")
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse")
    props.setProperty("ner.model", "edu/stanford/nlp/models/ner/spanish.ancora.distsim.s512.crf.ser.gz")
    props.setProperty("pos.model", "edu/stanford/nlp/models/pos-tagger/spanish/spanish-distsim.tagger")

    if (enable_Shift_reduce_parser)
      props.setProperty("parse.model", "edu/stanford/nlp/models/srparser/spanishSR.ser.gz")
    else
      props.setProperty("parse.model", "edu/stanford/nlp/models/lexparser/spanishPCFG.ser.gz")
    props
  }


  def parseSpanishDOC(xmlString: String) = {
    val mongoClient = MongoClient("10.176.148.60", 27017)
    val db = mongoClient("nlp_esp")
    val coll = db("nlp_text")

    val xml = loadString(xmlString)

    val text = (xml \\ "TEXT")
    val parse_sentence = (text \\ "P").map{
      sentence =>
        val trimmedSentence = sentence.text.trim.replaceAll("\n", " ")

        if (trimmedSentence.trim.isEmpty)
          ""
        else
        {
          if (trimmedSentence.charAt(trimmedSentence.length - 1) != '.')
            trimmedSentence + "."
          else
            trimmedSentence
        }
    }.mkString(" ")

    val doc_type = (xml \\ "@TYPE").text.trim()
    val doc_id = (xml \\ "@ID").text.trim()
    val headLine = (xml \\ "HEADLINE").text.trim()
    val dateLine = (xml \\ "DATELINE").text.trim()
    val result = parseForSpanish(parse_sentence, doc_type, doc_id,
      headLine, dateLine, false)


    val query = MongoDBObject("doc_id" -> doc_id)
    val doc_ = coll.findAndModify(query = query,
      update = result,
      upsert = true,
      fields = null,
      sort = null,
      remove = false,
      returnNew = true)


    val alldoc = coll.find(query)
    for (doc <- alldoc) println(doc)

    mongoClient.close()

  }

  def parseEnglishDOC(xmlString: String) = {
    val mongoClient = MongoClient("10.176.148.60", 27017)
    //val db = mongoClient("nlp_eng")
    val db = mongoClient("nlp_unit_test")
    val coll = db("nlp_text")

    val xml = loadString(xmlString)

    val text = (xml \\ "TEXT")
    val parse_sentence = (text \\ "P").map{
      sentence =>
        val trimmedSentence = sentence.text.trim.replaceAll("\n", " ")

        if (trimmedSentence.trim.isEmpty)
          ""
        else
          {
            if (trimmedSentence.charAt(trimmedSentence.length - 1) != '.')
              trimmedSentence + "."
            else
              trimmedSentence
          }
    }.mkString(" ")

    val doc_type = (xml \\ "@TYPE").text.trim()
    val doc_id = (xml \\ "@ID").text.trim()
    val headLine = (xml \\ "HEADLINE").text.trim()
    val dateLine = (xml \\ "DATELINE").text.trim()
    val result = parseForEnglish(parse_sentence, doc_type, doc_id,
      headLine, dateLine, false)


    val query = MongoDBObject("doc_id" -> doc_id)
    val doc_ = coll.findAndModify(query = query,
      update = result,
      upsert = true,
      fields = null,
      sort = null,
      remove = false,
      returnNew = true)


    val alldoc = coll.find(query)
    for (doc <- alldoc) println(doc)

    mongoClient.close()

  }

  def parseEnglishDOC_(xmlString: String): String = {

    val mongoClient = MongoClient("10.176.148.60", 27017)
    //val db = mongoClient("nlp_eng")
    val db = mongoClient("nlp_unit_test")
    val coll = db("nlp_text")

    val xml = loadString(xmlString)
    val text = (xml \\ "TEXT")
    val parse_sentence = (text \\ "P").map{
      sentence =>
        val trimmedSentence = sentence.text.trim.replaceAll("\n", " ")

        if (trimmedSentence.trim.isEmpty)
          ""
        else
        {
          if (trimmedSentence.charAt(trimmedSentence.length - 1) != '.')
            trimmedSentence + "."
          else
            trimmedSentence
        }
    }.mkString(" ")

    val doc_type = (xml \\ "@TYPE").text.trim()
    val doc_id = (xml \\ "@ID").text.trim()
    val headLine = (xml \\ "HEADLINE").text.trim()
    val dateLine = (xml \\ "DATELINE").text.trim()
    val result = parseForEnglish(parse_sentence, doc_type, doc_id,
      headLine, dateLine, false)


    val query = MongoDBObject("doc_id" -> doc_id)
    val doc_ = coll.findAndModify(query = query,
      update = result,
      upsert = true,
      fields = null,
      sort = null,
      remove = false,
      returnNew = true)

    mongoClient.close()

    parse_sentence
  }


  def parseForEnglish(sentence: String, doc_type: String, doc_id: String,
                      headLine: String, dateLine: String, enable_Shift_reduce_parser : Boolean): DBObject = {
    val pipeline = new StanfordCoreNLP(getPropertyForEnglish(enable_Shift_reduce_parser))
    val sent = new Annotation(sentence)
    pipeline.annotate(sent)

    var sentence_id = 0
    val parse_sentence = sent.get(classOf[SentencesAnnotation])
      .map { x =>

        // Sentence
        val trim_sentence = x.toString.trim
        if (trim_sentence.isEmpty)
          None
        else {

          var pennTree = ""
          try {
            // Parse Tree
            pennTree = x.get(classOf[TreeAnnotation]).toString
            //println(pennTree)
          } catch {
            case e: Exception => print("Exception in Penn Tree")
          }


          var dependencyTree = ""
          try {
            //Dependency Annotation Tree
            dependencyTree = x.get(classOf[CollapsedCCProcessedDependenciesAnnotation]).toString()

            //println("Dependency Tree")
            //println(dependencyTree)
          }
          catch {
            case e: Exception => print("Exception in Dependency Tree")
          }

          var token = ""
          try {
            // token
            token = x.get(classOf[TokensAnnotation]).map(x => (x.value().trim())).mkString(",")
            //println(token)
          } catch {
            case e: Exception => print("Exception in Collecting token")
          }

          var lemma = ""
          try {
            // lemma
            lemma = x.get(classOf[TokensAnnotation]).map(x => (x.lemma().trim())).mkString(",")
            //println(lemma)
          }
          catch {
            case e: Exception => print("Exception in Collecting lemma")
          }

          var ner = ""

          try {
            // ner
            ner = x.get(classOf[TokensAnnotation]).map(x => (x.ner(), x.value().trim()))
              .filter(x => (!"O".equals(x._1)))
              .groupBy(x => x._1)
              .map { kv =>
                val key = kv._1
                val value = kv._2
                val v = value.map(x => x._2).mkString("|")
                (key, v)

              }.toList.mkString(",")

            //println(ner)
          }
          catch {
            case e: Exception => print("Exception in Collecting ner")
          }


          var rel = ""


          /*try {
            // Get Relation
            rel = x.get(classOf[MachineReadingAnnotations.RelationMentionsAnnotation]).mkString
            //println(rel)
          }
          catch {
            case e: Exception => print("Exception in Collecting relation")
          }*/

          var sentiment = -1

          try {
            // Get Sentiment
            val tree = x.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
            val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
            //println(sentiment)
          }
          catch {
            case e: Exception => print("Exception in sentiment")
          }



          sentence_id += 1

          MongoDBObject("sentence_id" -> sentence_id,
            "sentence" -> trim_sentence,
            "parse_sentence" -> pennTree,
            "dependency_tree" -> dependencyTree,
            "token" -> token, "lemma" -> lemma, "ner" -> ner,
            "relation" -> rel, "sentiment" -> sentiment
          )
        }
      }


    var DCOREF = ""
    /*try {
      DCOREF = sent.get(classOf[CorefChainAnnotation]).toString
    } catch {
      case e: Exception => print("Exception in DCOREL")
    }*/

    //println(DCOREF)


    val builder = MongoDBObject.newBuilder
    builder += "type" -> doc_type

    builder += "doc_id" -> doc_id
    builder += "head_line" -> headLine
    builder += "date_line" -> dateLine
    builder += "sentences" -> parse_sentence
    builder += "corref" -> DCOREF

    builder.result()

  }

  def parseForSpanish(sentence: String, doc_type: String, doc_id: String,
                      headLine: String, dateLine: String, enable_Shift_reduce_parser: Boolean): DBObject = {
    val pipeline = new StanfordCoreNLP(getPropertyForSpanish(enable_Shift_reduce_parser))
    val sent = new Annotation(sentence)
    pipeline.annotate(sent)


    var sentence_id = 0
    val parse_sentence = sent.get(classOf[SentencesAnnotation])
      .map { x =>

        // Sentence
        val trim_sentence = x.toString.trim
        if (trim_sentence.isEmpty)
          None
        else {

          var pennTree = ""
          try {
            // Parse Tree
            pennTree = x.get(classOf[TreeAnnotation]).toString
            print(pennTree + "\n")
            x.get(classOf[TreeAnnotation]).pennPrint()


          } catch {
            case e: Exception => print("Exception in Penn Tree")
          }

          var token = ""
          try {
            // token
            token = x.get(classOf[TokensAnnotation]).map(x => (x.value().trim())).mkString(",")
            print(token + "\n")
          } catch {
            case e: Exception => print("Exception in Collecting token")
          }

          var lemma = ""
          try {
            // lemma
            lemma = x.get(classOf[TokensAnnotation]).map(x => (x.lemma().trim())).mkString(",")
            print(lemma + "\n")
          }
          catch {
            case e: Exception => print("Exception in Collecting lemma")
          }

          var ner = ""

          try {
            // ner
            ner = x.get(classOf[TokensAnnotation]).map(x => (x.ner(), x.value().trim()))
              .filter(x => (!"O".equals(x._1)))
              .groupBy(x => x._1)
              .map { kv =>
                val key = kv._1
                val value = kv._2
                val v = value.map(x => x._2).mkString("|")
                (key, v)

              }.toList.mkString(",")

            println(ner)
          }
          catch {
            case e: Exception => print("Exception in Collecting ner")
          }

          sentence_id += 1

          MongoDBObject("sentence_id" -> sentence_id,
            "sentence" -> trim_sentence,
            "parse_sentence" -> pennTree,
            "token" -> token, "lemma" -> lemma, "ner" -> ner
          )
        }
      }


    val builder = MongoDBObject.newBuilder
    builder += "type" -> doc_type

    builder += "doc_id" -> doc_id
    builder += "head_line" -> headLine
    builder += "date_line" -> dateLine
    builder += "sentences" -> parse_sentence

    builder.result()


  }


  def ExtractsDocInfo(doc_string: String, enable_Shift_reduce_parser: Boolean,
                      language:String): DBObject =
  {
    try {
      if (!doc_string.trim().isEmpty) {
        val trim_doc_str = doc_string.replace("\n", " ").trim
        if (trim_doc_str.startsWith("<DOC")) {
          // Split the sentence doc and text
          val textIndex = trim_doc_str.indexOf("<TEXT>")
          val doc_attr = trim_doc_str.substring(0, textIndex)


          val headLineIndex = doc_attr.indexOf("<HEADLINE>")
          val dateLineIndex = doc_attr.indexOf("<DATELINE>")

          var docSplitIndex = -1
          // Order is important
          var date_line = ""
          if (dateLineIndex != -1) {
            date_line = doc_attr.substring(dateLineIndex, doc_attr.indexOf("</DATELINE>"))
              .replace("<DATELINE>", " ").trim

            //println("DATELINE: " + date_line)

            docSplitIndex = dateLineIndex
          }


          var head_line = ""
          if (headLineIndex != -1) {
            head_line = doc_attr.substring(headLineIndex, doc_attr.indexOf("</HEADLINE>"))
              .replace("<HEADLINE>", " ").trim

            //println("HEADLINE: " + head_line)

            docSplitIndex = headLineIndex
          }


          val doc_text = {
            if (docSplitIndex == -1)
              doc_attr.trim.replace("<DOC", " ").stripSuffix(">").trim
            else
              doc_attr.substring(0, docSplitIndex).trim
                .replace("<DOC", " ").stripSuffix(">").trim
          }


          val attr = doc_text.split("\\s+|=")
          var i = 0
          var doc_id = ""
          var doc_type = ""

          while (i < attr.length) {
            if (attr(i).compareToIgnoreCase("ID") == 0) {
              doc_id = attr(i + 1).replace("\"", "")
              i = i + 2;
            }

            else if (attr(i).compareToIgnoreCase("TYPE") == 0) {
              doc_type = attr(i + 1).replace("\"", "")
              i = i + 2;
            }
            else
              i = i + 1
          }

          //println("DOC ID: " + doc_id + " TYPE: " + doc_type)


          if (doc_type.trim.compareToIgnoreCase("story") == 0)
            {
              /*val mongoClient = MongoClient("10.176.148.60", 27017)
              val db = mongoClient("nlp_unit_test1")
              val coll = db("nlp_text")

              val query = MongoDBObject("doc_id" -> doc_id)
              //val alldoc = coll.find(query)
              //mongoClient.close()

              //if (alldoc.size == 0)*/
                {
                  val text_ = trim_doc_str.substring(textIndex).replace("</DOC>", "")
                    .replace("<TEXT>", "").replace("</TEXT>", "")
                    .replace("<P>", "").split("</P>").map {

                    x =>
                      val s = x.trim

                      if (s.isEmpty)
                        s
                      else {
                        if (s.endsWith("."))
                          s
                        else
                          s + "."
                      }


                  }.mkString(" ")

                  //println(text_)


                  if (language == "ENG")
                    {
                      val result = parseForEnglish(text_, doc_type, doc_id,
                        head_line, date_line, enable_Shift_reduce_parser)
                      return  result
                    }
                  else if (language == "ESP")
                    {
                      val result = parseForSpanish(text_, doc_type, doc_id,
                        head_line, date_line, enable_Shift_reduce_parser)
                      return  result

                    }


                  //coll.insert(result)

                  //print(result.toString)
                  return null

                }
            }
        }
      }
    }
    catch
      {
        case e: Exception => print("Exception in Penn Tree: " + doc_string)
      }

       return null

    }


  def main (args: Array[String]){

    //val xml_String = "<DOC id=\"AFP_ENG_19940512.0038\" type=\"story\" ><DATELINE>reopens (INDIANAPOLIS)</DATELINE><TEXT><P>Spanish Open golf results MARDRID , May 15 -LRB- AFP -RRB- - Leading fourth and final round results from the Spanish Open here on Sunday -LRB- GB and Ire unless stated -RRB- : 277 Colin Montgomerie 70 71 66 70 278 Richard Boxall 69 69 70 70 , Mark Roe 70 68 69 71 , Mark McNulty -LRB- Zimbabwe -RRB- 68 69 70 71 279 Berhard Langer -LRB- Germany -RRB- 70 69 69 71 281 Ernie Els -LRB- S Africa -RRB- 67 74 73 67 , Jonathan Lomas 74 73 67 67 282 Seve Ballesteros -LRB- Spain -RRB- 72 71 73 66 , Steen Tinning -LRB- Denmark -RRB- 74 69 71 68 , Gordon Brand jnr 69 72 71 70 , Frederic Regard -LRB- France -RRB- 69 71 71 71 , Phil Price 72 72 67 71 , Jose-Maria Olazabal -LRB- Spain -RRB- 71 70 69 72 283 Peter Mitchell 68 74 69 72 , Manny Zerman -LRB- Italy -RRB- 70 73 70 70 284 Ross McFarlane 71 74 72 67 , Santiago Luna -LRB- Spain -RRB- 73 73 71 67 , Jesus Maria Arruti -LRB- Spain -RRB- 69 74 73 68 , Stuart Little 71 74 71 68 , Peter Teravainen -LRB- US -RRB- 73 69 71 71 , David Curry 71 69 72 72 285 Silvio Grappasonni -LRB- Italy -RRB- 75 72 71 67 , Domingo Hospital -LRB- Spain -RRB- 72 73 70 70 , Ricky Willison 71 71 72 71 286 Costantino Rocca -LRB- Italy -RRB- 73 69 74 70 , De Wet Basson -LRB- S Africa -RRB- 73 72 71 70 , Jose Coceres -LRB- Argentina -RRB- 69 74 69 74 , Stephen Field 70 70 69 77 287 Andrew Sherborne 69 75 73 70 288 Stephen Ames -LRB- Trinidad and Tobago -RRB- 72 75 72 69 , Andrew Collison 76 69 73 70 , Steven Bottomley 71 75 71 71 str/tl/94 .</P></TEXT>"
    //val xml_String = "<DOC id=\"AFP_ENG_20040708.0017R\" type=\"story\" ><HEADLINE>Parris Attack</HEADLINE><DATELINE>reopens (INDIANAPOLIS)</DATELINE><TEXT><P>Officials said that, in response to the attacks in Paris, the administration was seeking renewed global commitment to that intensified military action, and to a negotiated settlement of Syria's civil war.</P></TEXT></DOC>"

    //val xml_String = "<DOC id=\"AFP_ENG_20040708.0017R\" type=\"story\" ><HEADLINE>Parris Attack</HEADLINE><DATELINE>reopens (INDIANAPOLIS)</DATELINE><TEXT><P>Al Qaeda recupera el control de una segunda capital provincial en el Yemen.</P></TEXT></DOC>"
    val xml_String = "<DOC id=\"AFP_ENG_20040708.0017R\" type=\"story\" ><HEADLINE>Isarel</HEADLINE><DATELINE>reopens (INDIANAPOLIS)</DATELINE><TEXT><P>Israel said a mortar bomb was fired at it from the Gaza strip on Tuesday.</P></TEXT></DOC>"


    /*val  xml_String =
      """
        <DOC ID="AFP_ENG_19940512.0003" TYPE="multi" >
        <TEXT>


        Tributes poured in from around the world Thursday to the late LabourThe government in Damascus halted at the last minute talks betweensenior Israeli and Syrian army officers which were due to take placeearly November in Washington, a newspaper reported Thursday. The daily Haaretz said the officers had been set to join one of aseries of discreet meetings of the Israeli and Syrian ambassadors inthe US capital. The newspaper said the Syrians pulled out after Prime Minister YitzhakRabin publicly questioned the peaceful intentions of President Hafezal-Assad. However, Israel television's Second Channel reported Wednesday that anIsraeli general and several other Israeli officers did join on theregular sessions between ambassadors Itamar Rabinovich and WalidMuallam. The talks focussed on security arrangements on the Golan Heights aspart of a peace treaty between the two neighbours, the televisionsaid. The Israeli government and army refused to comment on the reports. With direct Israel-Syria peace negotiations blocked since February, USSecretary of State Warren Christopher is scheduled to visit the regionnext week for his seventh attempt this year to resolve differences.
        Party leader John Smith, who died earlier from a massive heart attack
        aged 55.

        </P>
        <P>
        In Washington, the US State Department issued a statement regretting
        "the untimely death" of the rapier-tongued Scottish barrister and
        parliamentarian.

        </TEXT>
        </DOC>
      """
      */


    var start_time = System.currentTimeMillis()
    //parseEnglishDOC(xml_String)
    ExtractsDocInfo(xml_String, true, "ENG")
    var timeInSecond = (System.currentTimeMillis() - start_time)
    println("\n\n")
    println("Total time (millisec) to process a document (Shift reduce parser): " + timeInSecond + "\n")



    start_time = System.currentTimeMillis()
    //parseEnglishDOC(xml_String)
    ExtractsDocInfo(xml_String, false,  "ENG")
    timeInSecond = (System.currentTimeMillis() - start_time)
    println("\n\n")
    println("Total time (millisec) to process a document: " + timeInSecond + "\n")
  }
}
