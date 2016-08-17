/**
 * Created by root on 10/8/15.
 */

import java.util.Properties

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import edu.stanford.nlp.dcoref.CorefCoreAnnotations.CorefChainAnnotation
import edu.stanford.nlp.ie.machinereading.structure.MachineReadingAnnotations
import edu.stanford.nlp.ling.CoreAnnotations.{SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation

import scala.collection.JavaConversions._

object ParseSentence {
  val props = new Properties()
  //props.setProperty("annotators", "tokenize, ssplit, cleanxml, pos, lemma, truecase, ner, regexner, parse, dcoref, sentiment, relation")


  //props.setProperty("regexner.mapping", "/usr/local/regexner.txt")

  //props.setProperty("tokenize.language", "es")
  props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref")
  //props.setProperty("ner.model", "edu/stanford/nlp/models/ner/spanish.ancora.distsim.s512.crf.ser.gz")
  //props.setProperty("pos.model", "edu/stanford/nlp/models/pos-tagger/spanish/spanish-distsim.tagger")
  //props.setProperty("parse.model", "edu/stanford/nlp/models/lexparser/spanishPCFG.ser.gz")

  val pipeline = new StanfordCoreNLP(props)



  def getPennParseTree(sentence: String): String = {

    try{
      val sent = new Annotation(sentence)
      pipeline.annotate(sent)
      sent.get(classOf[SentencesAnnotation])
        .head
        .get(classOf[TreeAnnotation])
        .toString
    } catch{
      case e:Exception => e.printStackTrace()
        print(sentence)
        ""
    }
  }

  def getDParseTree(sentence: String): String = {
    val sent = new Annotation(sentence)
    pipeline.annotate(sent)
    sent.get(classOf[SentencesAnnotation])
      .head
      .get(classOf[CollapsedCCProcessedDependenciesAnnotation])
      .toString

  }


  def getTOKENS(sentence: String):String = {
    val sent = new Annotation(sentence)
    pipeline.annotate(sent)
    sent.get(classOf[SentencesAnnotation])
      .head
      .get(classOf[TokensAnnotation]).map(x => (x.value().trim()))
      .toList.mkString(",")

  }


  def getNER(sentence: String):String = {

    try {
      val sent = new Annotation(sentence)
      pipeline.annotate(sent)
      sent.get(classOf[SentencesAnnotation])
        .head
        .get(classOf[TokensAnnotation]).map(x => (x.ner(), x.value().trim()))
        .filter(x => (!"O".equals(x._1)))
        .groupBy(x => x._1)
        .map { kv =>
          val key = kv._1
          val value = kv._2
          val v = value.map(x => x._2).mkString("|")
          (key, v)

        }.toList.mkString(",")
    } catch{
      case e:Exception => e.printStackTrace()
        print(sentence)
        ""
    }
  }

  def getLEMMA(sentence: String):String = {
    val sent = new Annotation(sentence)
    pipeline.annotate(sent)
    sent.get(classOf[SentencesAnnotation])
      .head
      .get(classOf[TokensAnnotation]).map(x => (x.lemma(), x.value().trim())).mkString(",")
  }


  def getSentiment(sentence: String):String = {
    val sent = new Annotation(sentence)
    pipeline.annotate(sent)
    sent.get(classOf[SentencesAnnotation])
      .map{ x =>
        val tree=  x.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
        RNNCoreAnnotations.getPredictedClass(tree)
      }.mkString(",")
  }


  def getDCOREF(sentence: String):String = {
    val sent = new Annotation(sentence)
    pipeline.annotate(sent)
    sent.get(classOf[CorefChainAnnotation]).toString
  }

  def getRELATION(sentence: String):String = {
    val sent = new Annotation(sentence)
    pipeline.annotate(sent)
    sent.get(classOf[SentencesAnnotation])
      .map{ x =>
        x.get(classOf[MachineReadingAnnotations.RelationMentionsAnnotation]).mkString
      }.mkString("\n")
  }

  def parseAndStoreSentenceToMongoDB(sentence: String): String = {
    val mongoClient = MongoClient("10.176.148.60", 27017)
    val db = mongoClient("nlp_unit_test")
    val coll = db("nlp_text")

    val parseSentence = getPennParseTree(sentence)

    val dbObj = MongoDBObject("sentence" -> sentence,
      "parse_sentence" -> parseSentence)

    coll.save(dbObj)
    mongoClient.close()

    parseSentence
  }





  def main (args: Array[String]){

    print(parseAndStoreSentenceToMongoDB("I am rakib"))

  }
}
