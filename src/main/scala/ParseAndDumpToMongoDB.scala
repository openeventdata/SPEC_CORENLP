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

object ParseAndDumpToMongoDB extends  Serializable{

  def getPropertyForEnglish(enable_Shift_reduce_parser: Boolean): Properties = {

    val props = new Properties()
    //props.setProperty("annotators", "tokenize, ssplit, cleanxml, pos, lemma, truecase, ner, parse, dcoref, sentiment")

    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref, sentiment")

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
            /*ner = x.get(classOf[TokensAnnotation]).map(x => (x.ner(), x.value().trim()))
              .filter(x => (!"O".equals(x._1)))
              .groupBy(x => x._1)
              .map { kv =>
                val key = kv._1
                val value = kv._2
                val v = value.map(x => x._2).mkString("|")
                (key, v)

              }.toList.mkString(",")*/

            val tokens = x.get(classOf[TokensAnnotation])

            var previousNERType = ""

            val NERString = new StringBuilder()

            for (a <- tokens)
            {
              var currentNERType = a.ner()

              if (!"O".equals(a.ner()))
              {
                val textValue = a.value().replaceAll(",", "").replaceAll("$", "")


                if (!previousNERType.equals(currentNERType)) {
                  NERString.append("##")
                  NERString.append(a.ner() + ":" + textValue)
                }
                else
                  NERString.append(" " + textValue)
              }
              else
                NERString.append("##")
              previousNERType = currentNERType
            }

            ner = NERString.toString.split("##").filter(x => !x.isEmpty).
              map{
                x =>
                  val nerSplit = x.split(':')
                  (nerSplit(0), nerSplit(1))
              }.groupBy(x => x._1)
              .map { kv =>
                val key = kv._1
                val value = kv._2
                val v = value.map(x => x._2).mkString("|")
                (key, v)

              }.toList.mkString(",")

            //println(ner)
          }
          catch {
            case e: Exception => print("Exception in Collecting ner for ENG for : " + trim_sentence + "\n\n")
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
    builder += "sentences" -> parse_sentence.toArray
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

//  def ExtractDocInfoFromJson(jsonString: String, enable_Shift_reduce_parser: Boolean,
//                             language:String): DBObject =
//  {
//    val parsed = JSON.parseFull(jsonString)
//
//
//
//    if (parsed("language") == "ENG" || language == "english")
//    {
//      val result = parseForEnglish(text_, doc_type, doc_id,
//        head_line, date_line, enable_Shift_reduce_parser)
//      return  result
//    }
//    else if (language == "ESP")
//    {
//      val result = parseForSpanish(text_, doc_type, doc_id,
//        head_line, date_line, enable_Shift_reduce_parser)
//      return  result
//
//    }
//
//  }


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

    //val xml_String = "<DOC id=\"AFP_ENG_19940512.0038\" type=\"story\" ><TEXT><P>Bill clinton helped donald Trum at United States.</P></TEXT></DOC>"
    val xml_String = "<DOC id=\"APW_ENG_20081222.0377\" type=\"story\" > <HEADLINE> Tata Motors keeps spending on Jaguar Land Rover </HEADLINE> <DATELINE> MUMBAI, India 2008-12-22 12:13:44 UTC </DATELINE> <TEXT> <P> Tata Motors Ltd. has been spending money on its Jaguar Land Rover subsidiary and will continue to do so as needed, a Tata spokesman said Monday after media reports that the Indian auto giant had agreed to infuse the unit with tens of millions of pounds to avert a cash flow crisis. </P> <P> \"The company is spending whatever is required to the best of its abilities,\" said Tata Motors spokesman Debasis Ray. \"A company has to run. We are doing everything to make the company run.\" </P> <P> He declined to say how much Tata Motors has diverted to Jaguar Land Rover since it bought the brands from Ford Motor Co. for $2.3 billion in June. Since then, car sales have plummeted around the world, and governments have stepped in to prop up failing companies in an effort to stem job losses. </P> <P> The Financial Times reported Monday that Tata Motors had agreed to inject \"tens of millions\" of pounds into Jaguar Land Rover. </P> <P> Unlike other struggling global automakers, Tata Motors has an advantage: It is part of one of India's oldest and largest family conglomerates, whose combined holdings account for about 4 percent of the entire market capitalization of the Bombay Stock Exchange. </P> <P> The Tata group has already stepped in once to help refinance part of the $3 billion bridge loan Tata Motors used to fund the Jaguar Land Rover acquisition, and some analysts expect it may well do so again. </P> <P> \"We do expect commitment from the group to support Tata Motors,\" said Naresh Takkar, Managing Director of ICRA, a Moody's affiliate based in New Delhi. </P> <P> The Tata group of companies, which owns 41.82 percent of Tata Motors, has interests in steel, finance, outsourcing, hotels, and telecom, among other things. Its holdings include Britain's Tetley Tea and Corus Steel, and New York's Pierre Hotel. </P> <P> Ray declined to speculate on whether the Tata group might step in with further financing for its auto subsidiary. </P> <P> Meanwhile, Tata Motors is turning to another lender of last resort: The British government. </P> <P> U.K. Business Secretary Peter Mandelson said last week that the government was in talks with Tata about a possible bailout for Jaguar Land Rover \"because they argue that they are under particular strain.\" </P> <P> Ray declined to comment on government negotiations. He did say, \"Governments all over the world are supporting their auto industries. . .The auto industry in the U.K. also deserves support. Jaguar Land Rover is part of that industry.\" </P> <P> Jaguar Land Rover has about 16,000 employees in the U.K. and spends 400 million pounds a year on research and development and 2.5 billion pounds on suppliers. </P> <P> Its sales from June to September slipped 11.8 percent by volume from the year-earlier period. </P> <P> Two weeks ago, Tata Motors took the extraordinary step of appealing directly to the public for funds, offering fixed deposit rates from 10 percent to 12.83 percent. </P> <P> Analysts say Tata Motors' cash needs are critical. </P> <P> Credit Suisse said that in addition to funding the balance of the bridge loan, which comes due in June 2009, Tata Motors will need to raise $2.4 billion through fiscal year 2010 to keep up with its own operating expenses and capital expenditure plans. </P> <P> Ray declined to comment on Tata's fundraising needs. </P> <P> \"We have no doubt in our minds we will meet our obligations. That is not an issue,\" he said. </P> </TEXT> </DOC>Exception in Penn Tree: <DOC id=\"APW_ENG_20081220.0315\" type=\"story\" > <HEADLINE> Irish bargain-hunters invade Northern Ireland </HEADLINE> <DATELINE> NEWRY, Northern Ireland 2008-12-20 14:48:20 UTC </DATELINE> <TEXT> <P> Recession fears across Europe have consumers spending less and retailers fearful of the future. But one Northern Ireland border town is enjoying the biggest shopping spree in its history. </P> <P> A record-strong euro and deepening recession in the neighboring Republic of Ireland have turned Newry in recent months into the most intensively shopped spot in Ireland -- if not the continent. The 5-mile (8-km) traffic jams and patience-shattering hunts for a parking spot are already the talk of the island. </P> <P> The phenomenon could reach its peak this weekend before Christmas as tens of thousands travel from up to 12 hours' drive away to cash in on Northern Ireland shops pricing goods in record-cheap British pounds. </P> <P> \"No, never been to the 'black north' before. Never seen any reason to come,\" said Sean Magee, 35, a short-of-work construction worker from faraway Limerick, southwest Ireland, in the parking lot of Newry's glitziest shopping center, the 55-shop Quays. </P> <P> Magee bore the broadest of smiles and the fullest of shopping carts. He and his two friends, who had traveled eight hours by work van the night before and slept rough in the back, were pushing similar loads of beer, cider and liquor -- much of it produced in the Irish Republic yet available for less than half the price in Northern Ireland. </P> <P> \"Never bought so much booze in one go before, but you'd be crazy not to. Think I'm good 'til St. Pat's,\" Magee said, referring to Ireland's national holiday of St. Patrick's Day on March 17. \"And this is sure to be a New Year's to remember!\" </P> <P> Then he donned his best Arnold Schwarzenegger-as-Terminator accent and cast a cold eye back on the shopping center. \"I'll be back,\" he said to laughter all around. </P> <P> Veteran shoppers, store owners and retail experts long have watched the ebb and flow of shoppers across Ireland's border. Different sales-tax policies and the shifting values between the north's British pound versus the euro -- and, before 2002, the old Irish punt -- usually have meant particular goods were cheaper on one side than the other. </P> <P> But never like this since Ireland's partition in 1921. These days, about the only thing cheaper in the south -- increasingly decried by shoppers as \"the Rip-off Republic\" -- is the vehicle fuel required to make the trip north. </P> <P> Several months ago, the pound was worth 50 percent more than the euro, yet many British-priced goods already were cheaper than in the independent south. That reflects the better economies of scale and higher commercial competition in the United Kingdom versus the Irish Republic. </P> <P> Today, thanks to a perfect storm of cross-border contrasts -- the euro is approaching parity in value with the pound, the British have cut sales tax while the Irish have raised theirs, and British retailers are slashing prices because of recession in Britain rather than boom in Northern Ireland -- savings for north-bound shoppers are at least 30 percent and usually more, depending on what you're buying. </P> <P> For Fiona O'Mahony, a mother of two from the Dublin suburbs, it's all about the nappies, a.k.a. diapers. </P> <P> \"Pampers are a big part of the household budget these days. It's not festive, but it's reality,\" said O'Mahony, 32, whose cart was full of diapers, formula and children's clothes. </P> <P> O'Mahony left behind the kids with hubbie for a cross-border raid with her girlfriends, who traveled up by convoy to ensure they could carry a maximum load back. They had debated whether to fly to New York City for Christmas shopping -- like they did at least annually, cashing in on the weak U.S. dollar during Ireland's Celtic Tiger boom that died last year -- but decided that Ulster was a better fit for newfound recession. </P> <P> \"No more 'Sex and the City' for us,\" she quipped. Instead, Belfast will substitute for New York as the girls planned deeper excursions into British territory over the weekend, reaching the mecca of many southern explorers -- Ireland's only Ikea, east of Belfast -- on Sunday. </P> <P> \"We'll never make it. We'll never have the room. I'll have to post one of the girls back to Dublin,\" she said. </P> <P> The daily battle of Newry begins at dawn, as shoppers leave their hotel rooms -- like gold dust at the moment -- or arrive before 8 a.m. openings in vain hope of beating the build-up of traffic back to the border a few miles (kilometers) away. </P> <P> Peter Murray has been general manager of Newry's oldest shopping center, the 60-shop Buttercrane, for the past 20 years and seen good times and bad -- and nothing like this. </P> <P> \"Newry is bucking all the doom and gloom thanks to the biting recession down south and the amazing power of the euro,\" Murray said. </P> <P> He said the Buttercrane alone was getting about 200,000 shoppers a week -- this in a city with a population under 50,000 -- and shops were reporting 125 percent growth in sales from customers using euros. </P> </TEXT> </DOC>Exception in Penn Tree: <DOC id=\"APW_ENG_20081220.0168\" type=\"story\" > <HEADLINE> NZ makes good start in reply to WIndies </HEADLINE> <DATELINE> NAPIER, New Zealand 2008-12-20 09:18:36 UTC </DATELINE> <TEXT> <P> Daniel Flynn and Tim McIntosh made 50s in a century second-wicket stand on Saturday as New Zealand made a good start to chasing down West Indies first innings on day two of the second test. </P> <P> At stumps, New Zealand was 145-2 in reply to the West Indies' 307 at McLean Park, giving the hosts the upper hand in the slow-moving match affected late Saturday by a short rain delay. </P> <P> Flynn was out for 57 close to stumps, his second half century in tests after his breakthrough innings of 95 in the drawn first test. </P> <P> The left-handed opener McIntosh, a painstaking batsmen finding his feet in the test arena, had reached his highest test score of 62 not out. </P> <P> \"These two have set a base that our middle and lower order should be able to launch from,\" New Zealand coach Andy Moles said. </P> <P> McIntosh will resume Sunday with Ross Taylor (4 not out). West Indies paceman Fidel Edwards claimed both wickets to have 2-26 by stumps. </P> <P> Flynn and McIntosh re-established the innings after the loss of opener Jamie How for 12 when the total was 19. </P> <P> Confronted by tight bowling -- a West Indies attack determined to exert pressure through containment -- the pair took 30.2 overs to raise New Zealand's 50 and 98 minutes to post their 50 partnership. </P> <P> Flynn reached his half century in 134 minutes with four fours and two sixes and McIntosh had batted more than four and a half hours for his 62 runs by stumps. </P> <P> He owed his continued presence at the crease to an almost comical mix up between Edwards and wicketkeeper Denesh Ramdin when he was 14 and New Zealand was 36. </P> <P> Attempting to pull a bouncer from Edwards, McIntosh succeeded only in hitting the ball straight up in the air but as wicketkeeper and bowler converged. Both called but neither attempted the catch. </P> <P> Earlier, Iain O'Brien claimed the last four West Indies wickets to finish with a career-best 6-75. </P> <P> \"It's as consistent as I've ever been, pace-wise as well,\" O'Brien said. \"It was nice to be able to hit areas I wanted to.\" </P> <P> Shivnarine Chanderpaul, who resumed on 100, finished unbeaten on 126 after batting for five hours and 40 minutes. </P> <P> The not out innings was enough to lift his career average above 50, and extended his remarkable record over the past 18 months in which he has averaged 103 in tests. </P> <P> Remarkably, his innings in Napier was his highest score and first century against New Zealand in tests, surpassing his 82 at Bridgetown in 1996. </P> <P> He has become so hard to remove since the England series last year that the average duration of his completed innings in that 18-month period has been 6 1/2 hours. </P> </TEXT> </DOC>Exception in Penn Tree: <DOC id=\"APW_ENG_20081222.0050\" type=\"story\" > <HEADLINE> 2nd test: New Zealand-West Indies scoreboard </HEADLINE> <DATELINE> NAPIER, New Zealand 2008-12-22 01:07:35 UTC </DATELINE> <TEXT> <P> Scoreboard at lunch Monday on the fourth day of the second cricket test between New Zealand and the West Indies at McLean Park: </P> <P> West Indies 1st Innings: 307 </P> <P> New Zealand 1st Innings: 371 </P> <P> West Indies, 2nd Innings </P> <P> (Overnight: 62-2) </P> <P> Chris Gayle not out 83 </P> <P> Sewnarine Chattergoon c Taylor b Patel 25 </P> <P> Ramnaresh Sarwan lbw b Vettori 1 </P> <P> Xavier Marshall c Taylor b Patel 18 </P> <P> Shivnarine Chanderpaul c &amp; b Patel 0 </P> <P> Brendan Nash not out 18 </P> <P> Extras: (1nb) 1 </P> <P> TOTAL: (for four wickets) 146 </P> <P> Overs: 53. Batting time: 187 minutes. </P> <P> Still to bat: Denesh Ramdin, Jerome Taylor, Sulieman Benn, Daren Powell, Fidel Edwards. </P> <P> Fall of wickets: 1-58, 2-62, 3-106, 4-106. </P> <P> Bowling: Iain O'Brien 9-1-49-0 (1nb), Kyle Mills 2-0-14-0, Jeetan Patel 21-7-59-3, Daniel Vettori 21-10-24-1. </P> <P> Umpires: Rudi Koertzen, South Africa, and Ameish Saheba, India. </P> <P> Match referee: Javagal Srinath, India. Television umpire: Mark Benson, England. </P> <P> Toss: won by West Indies. </P> <P> Series: 2-match series tied 0-0. </P> </TEXT> </DOC>"
    //val xml_String = "<DOC id=\"APW_ENG_20081222.0377\" type=\"story\" > <HEADLINE> Tata Motors keeps spending on Jaguar Land Rover </HEADLINE> <DATELINE> MUMBAI, India 2008-12-22 12:13:44 UTC </DATELINE> <TEXT> <P> Tata Motors Ltd. has been spending money on its Jaguar Land Rover subsidiary and will continue to do so as needed, a Tata spokesman said Monday after media reports that the Indian auto giant had agreed to infuse the unit with tens of millions of pounds to avert a cash flow crisis. </P> <P> \"The company is spending whatever is required to the best of its abilities,\" said Tata Motors spokesman Debasis Ray. \"A company has to run. We are doing everything to make the company run.\" </P> <P> He declined to say how much Tata Motors has diverted to Jaguar Land Rover since it bought the brands from Ford Motor Co. for $2.3 billion in June. Since then, car sales have plummeted around the world, and governments have stepped in to prop up failing companies in an effort to stem job losses. </P> <P> The Financial Times reported Monday that Tata Motors had agreed to inject \"tens of millions\" of pounds into Jaguar Land Rover. </P> <P> Unlike other struggling global automakers, Tata Motors has an advantage: It is part of one of India's oldest and largest family conglomerates, whose combined holdings account for about 4 percent of the entire market capitalization of the Bombay Stock Exchange. </P> <P> The Tata group has already stepped in once to help refinance part of the $3 billion bridge loan Tata Motors used to fund the Jaguar Land Rover acquisition, and some analysts expect it may well do so again. </P> <P> \"We do expect commitment from the group to support Tata Motors,\" said Naresh Takkar, Managing Director of ICRA, a Moody's affiliate based in New Delhi. </P> <P> The Tata group of companies, which owns 41.82 percent of Tata Motors, has interests in steel, finance, outsourcing, hotels, and telecom, among other things. Its holdings include Britain's Tetley Tea and Corus Steel, and New York's Pierre Hotel. </P> <P> Ray declined to speculate on whether the Tata group might step in with further financing for its auto subsidiary. </P> <P> Meanwhile, Tata Motors is turning to another lender of last resort: The British government. </P> <P> U.K. Business Secretary Peter Mandelson said last week that the government was in talks with Tata about a possible bailout for Jaguar Land Rover \"because they argue that they are under particular strain.\" </P> <P> Ray declined to comment on government negotiations. He did say, \"Governments all over the world are supporting their auto industries. . .The auto industry in the U.K. also deserves support. Jaguar Land Rover is part of that industry.\" </P> <P> Jaguar Land Rover has about 16,000 employees in the U.K. and spends 400 million pounds a year on research and development and 2.5 billion pounds on suppliers. </P> <P> Its sales from June to September slipped 11.8 percent by volume from the year-earlier period. </P> <P> Two weeks ago, Tata Motors took the extraordinary step of appealing directly to the public for funds, offering fixed deposit rates from 10 percent to 12.83 percent. </P> <P> Analysts say Tata Motors' cash needs are critical. </P> <P> Credit Suisse said that in addition to funding the balance of the bridge loan, which comes due in June 2009, Tata Motors will need to raise $2.4 billion through fiscal year 2010 to keep up with its own operating expenses and capital expenditure plans. </P> <P> Ray declined to comment on Tata's fundraising needs. </P> <P> \"We have no doubt in our minds we will meet our obligations. That is not an issue,\" he said. </P> </TEXT> </DOC>Exception in Penn Tree: <DOC id=\"APW_ENG_20081220.0315\" type=\"story\" > <HEADLINE> Irish bargain-hunters invade Northern Ireland </HEADLINE> <DATELINE> NEWRY, Northern Ireland 2008-12-20 14:48:20 UTC </DATELINE> <TEXT> <P> Recession fears across Europe have consumers spending less and retailers fearful of the future. But one Northern Ireland border town is enjoying the biggest shopping spree in its history. </P> <P> A record-strong euro and deepening recession in the neighboring Republic of Ireland have turned Newry in recent months into the most intensively shopped spot in Ireland -- if not the continent. The 5-mile (8-km) traffic jams and patience-shattering hunts for a parking spot are already the talk of the island. </P> <P> The phenomenon could reach its peak this weekend before Christmas as tens of thousands travel from up to 12 hours' drive away to cash in on Northern Ireland shops pricing goods in record-cheap British pounds. </P> <P> \"No, never been to the 'black north' before. Never seen any reason to come,\" said Sean Magee, 35, a short-of-work construction worker from faraway Limerick, southwest Ireland, in the parking lot of Newry's glitziest shopping center, the 55-shop Quays. </P> <P> Magee bore the broadest of smiles and the fullest of shopping carts. He and his two friends, who had traveled eight hours by work van the night before and slept rough in the back, were pushing similar loads of beer, cider and liquor -- much of it produced in the Irish Republic yet available for less than half the price in Northern Ireland. </P> <P> \"Never bought so much booze in one go before, but you'd be crazy not to. Think I'm good 'til St. Pat's,\" Magee said, referring to Ireland's national holiday of St. Patrick's Day on March 17. \"And this is sure to be a New Year's to remember!\" </P> <P> Then he donned his best Arnold Schwarzenegger-as-Terminator accent and cast a cold eye back on the shopping center. \"I'll be back,\" he said to laughter all around. </P> <P> Veteran shoppers, store owners and retail experts long have watched the ebb and flow of shoppers across Ireland's border. Different sales-tax policies and the shifting values between the north's British pound versus the euro -- and, before 2002, the old Irish punt -- usually have meant particular goods were cheaper on one side than the other. </P> <P> But never like this since Ireland's partition in 1921. These days, about the only thing cheaper in the south -- increasingly decried by shoppers as \"the Rip-off Republic\" -- is the vehicle fuel required to make the trip north. </P> <P> Several months ago, the pound was worth 50 percent more than the euro, yet many British-priced goods already were cheaper than in the independent south. That reflects the better economies of scale and higher commercial competition in the United Kingdom versus the Irish Republic. </P> <P> Today, thanks to a perfect storm of cross-border contrasts -- the euro is approaching parity in value with the pound, the British have cut sales tax while the Irish have raised theirs, and British retailers are slashing prices because of recession in Britain rather than boom in Northern Ireland -- savings for north-bound shoppers are at least 30 percent and usually more, depending on what you're buying. </P> <P> For Fiona O'Mahony, a mother of two from the Dublin suburbs, it's all about the nappies, a.k.a. diapers. </P> <P> \"Pampers are a big part of the household budget these days. It's not festive, but it's reality,\" said O'Mahony, 32, whose cart was full of diapers, formula and children's clothes. </P> <P> O'Mahony left behind the kids with hubbie for a cross-border raid with her girlfriends, who traveled up by convoy to ensure they could carry a maximum load back. They had debated whether to fly to New York City for Christmas shopping -- like they did at least annually, cashing in on the weak U.S. dollar during Ireland's Celtic Tiger boom that died last year -- but decided that Ulster was a better fit for newfound recession. </P> <P> \"No more 'Sex and the City' for us,\" she quipped. Instead, Belfast will substitute for New York as the girls planned deeper excursions into British territory over the weekend, reaching the mecca of many southern explorers -- Ireland's only Ikea, east of Belfast -- on Sunday. </P> <P> \"We'll never make it. We'll never have the room. I'll have to post one of the girls back to Dublin,\" she said. </P> <P> The daily battle of Newry begins at dawn, as shoppers leave their hotel rooms -- like gold dust at the moment -- or arrive before 8 a.m. openings in vain hope of beating the build-up of traffic back to the border a few miles (kilometers) away. </P> <P> Peter Murray has been general manager of Newry's oldest shopping center, the 60-shop Buttercrane, for the past 20 years and seen good times and bad -- and nothing like this. </P> <P> \"Newry is bucking all the doom and gloom thanks to the biting recession down south and the amazing power of the euro,\" Murray said. </P> <P> He said the Buttercrane alone was getting about 200,000 shoppers a week -- this in a city with a population under 50,000 -- and shops were reporting 125 percent growth in sales from customers using euros. </P> </TEXT> </DOC>Exception in Penn Tree: <DOC id=\"APW_ENG_20081220.0168\" type=\"story\" > <HEADLINE> NZ makes good start in reply to WIndies </HEADLINE> <DATELINE> NAPIER, New Zealand 2008-12-20 09:18:36 UTC </DATELINE> <TEXT> <P> Daniel Flynn and Tim McIntosh made 50s in a century second-wicket stand on Saturday as New Zealand made a good start to chasing down West Indies first innings on day two of the second test. </P> <P> At stumps, New Zealand was 145-2 in reply to the West Indies' 307 at McLean Park, giving the hosts the upper hand in the slow-moving match affected late Saturday by a short rain delay. </P> <P> Flynn was out for 57 close to stumps, his second half century in tests after his breakthrough innings of 95 in the drawn first test. </P> <P> The left-handed opener McIntosh, a painstaking batsmen finding his feet in the test arena, had reached his highest test score of 62 not out. </P> <P> \"These two have set a base that our middle and lower order should be able to launch from,\" New Zealand coach Andy Moles said. </P> <P> McIntosh will resume Sunday with Ross Taylor (4 not out). West Indies paceman Fidel Edwards claimed both wickets to have 2-26 by stumps. </P> <P> Flynn and McIntosh re-established the innings after the loss of opener Jamie How for 12 when the total was 19. </P> <P> Confronted by tight bowling -- a West Indies attack determined to exert pressure through containment -- the pair took 30.2 overs to raise New Zealand's 50 and 98 minutes to post their 50 partnership. </P> <P> Flynn reached his half century in 134 minutes with four fours and two sixes and McIntosh had batted more than four and a half hours for his 62 runs by stumps. </P> <P> He owed his continued presence at the crease to an almost comical mix up between Edwards and wicketkeeper Denesh Ramdin when he was 14 and New Zealand was 36. </P> <P> Attempting to pull a bouncer from Edwards, McIntosh succeeded only in hitting the ball straight up in the air but as wicketkeeper and bowler converged. Both called but neither attempted the catch. </P> <P> Earlier, Iain O'Brien claimed the last four West Indies wickets to finish with a career-best 6-75. </P> <P> \"It's as consistent as I've ever been, pace-wise as well,\" O'Brien said. \"It was nice to be able to hit areas I wanted to.\" </P> <P> Shivnarine Chanderpaul, who resumed on 100, finished unbeaten on 126 after batting for five hours and 40 minutes. </P> <P> The not out innings was enough to lift his career average above 50, and extended his remarkable record over the past 18 months in which he has averaged 103 in tests. </P> <P> Remarkably, his innings in Napier was his highest score and first century against New Zealand in tests, surpassing his 82 at Bridgetown in 1996. </P> <P> He has become so hard to remove since the England series last year that the average duration of his completed innings in that 18-month period has been 6 1/2 hours. </P> </TEXT> </DOC>Exception in Penn Tree: <DOC id=\"APW_ENG_</TEXT> </DOC>"
    //val xml_String = "<DOC id=\"AFP_ENG_20040708.0017R\" type=\"story\" ><HEADLINE>Parris Attack</HEADLINE><DATELINE>reopens (INDIANAPOLIS)</DATELINE><TEXT><P>Officials said that, in response to the attacks in Paris, the administration was seeking renewed global commitment to that intensified military action, and to a negotiated settlement of Syria's civil war.</P></TEXT></DOC>"

    //val xml_String = "<DOC id=\"AFP_ENG_20040708.0017R\" type=\"story\" ><HEADLINE>Parris Attack</HEADLINE><DATELINE>reopens (INDIANAPOLIS)</DATELINE><TEXT><P>Al Qaeda recupera el control de una segunda capital provincial en el Yemen.</P></TEXT></DOC>"
    //val xml_String = "<DOC id=\"AFP_ENG_20040708.0017R\" type=\"story\" ><HEADLINE>Isarel</HEADLINE><DATELINE>reopens (INDIANAPOLIS)</DATELINE><TEXT><P>Israel said a mortar bomb was fired at it from the Gaza strip on Tuesday.</P></TEXT></DOC>"


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


    println(ExtractsDocInfo(xml_String, true, "ENG").toMap())

    /*var start_time = System.currentTimeMillis()
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
    */
  }
}
