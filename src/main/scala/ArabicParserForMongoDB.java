/**
 * Created by root on 11/30/15.
 */
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import edu.stanford.nlp.international.arabic.process.ArabicTokenizer;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.parser.shiftreduce.ShiftReduceParser;
import edu.stanford.nlp.process.Tokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.StringUtils;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

public class ArabicParserForMongoDB {

    public static void main(String[] args) throws FileNotFoundException {
        ArabicParserForMongoDB obj = new ArabicParserForMongoDB();
        String xml_String = "<DOC id=\"XIN_ARB_20080901.0001\" type=\"story\" >  <HEADLINE>  ????? ?????? ????? ??????  </HEADLINE>  <TEXT>  <P>  ????? ?????? ????? ??????  </P>  <P>  ???? ??? ?????? 2008 (??????) ???? ??? ?????? ?? ??????? ????????  ??????? ????????:  </P>  <P>    </P>  <P>  ????? -- ???? ??????? ??? ????? ?? ???? ?????? ????? ??? ???????  ???? ??? 630 ??? ????? ???? ???? ??? ???? ????? ????? \"?????? ??  ??? ??????\" ?????? ???? ????? ?????? ?????? ????????? ???????  ?????????.  </P>  <P>  ??? ??? ?? ??? ???????? ??? ????? ??????? ?? ??????? ??? ??  ??????? ???????? ???? ??? ?????? ??????? ???????? ????? ?????  ??????? ????????. ????? ???? ?????? ???? ??? ?????? ?????? ??????  \"?????? ?? ??? ??????\" ??? ???? ?????? ????.  </P>  <P>  ????? ?????? ????? ???? ??? ???? ?????? ???? ????? ??? ???? ????  ?? ???? ???? ????? ????? ?? ??????? ??? ???? ???? ?? ?????? ???  ??? ?? ??????? ?? ????? ???? ????? ?????? ??????? ???????  ??????????? ???? ?????.  </P>  <P>  ????? -- ????? ????? ??????? ??????? ??? ????? ????? ???????  ???????? ????? ??????? ????? ????????? ?? ???? ??? ?????? ??  ????? ????? ?????.  </P>  <P>  ???? ????? ???? ??????? ???? ?????? ?? ??????? ?????? ?????  ?????? ?? ????? ?????? ???? ???? ???? ???? ??? ????? ???? ???????  ??? ????? ?????? ???????? ?????? ?? ?????? ??????.  </P>  <P>  ????? ???? ????????? ???????? ?? ?????? ??? ??? ???? ????? ??  ?????? ?? ????? ?????? ?????? ??????? ??? ????? ?? ??? ???? ??  ??????? ???? ?? ????? ???? ????? ???? ???. ????? ????? ??????  ????? ?????? ????? ????? ???? ?? ?????? ????. (????)  </P>  </TEXT>  </DOC>";
        DBObject result = obj.parseArabicDoc(xml_String, false, "arabicFactored.ser.gz","arabic.tagger");
        System.out.println(result.toString());
        result = obj.parseArabicDoc(xml_String, true, "arabicSR.ser.gz","arabic.tagger");
        System.out.println(result.toString());
    }

    public DBObject parseArabicDoc(String doc_string, boolean shiftReduceParsing, String parserModelPath, String taggerPath) throws FileNotFoundException {
        if (!doc_string.trim().isEmpty()) {
            String trim_doc_str = doc_string.replace("\n", " ").trim();
            if (trim_doc_str.startsWith("<DOC")) {
                // Split the sentence doc and text
                int textIndex = trim_doc_str.indexOf("<TEXT>");
                String doc_attr = trim_doc_str.substring(0, textIndex);

                int headLineIndex = doc_attr.indexOf("<HEADLINE>");
                int dateLineIndex = doc_attr.indexOf("<DATELINE>");

                int docSplitIndex = -1; // Order is important
                String date_line = "";
                if (dateLineIndex >= 0) {
                    date_line = doc_attr.substring(dateLineIndex, doc_attr.indexOf("</DATELINE>")).replace("<DATELINE>", " ").trim();
                    System.out.println("DATELINE: " + date_line);
                    docSplitIndex = dateLineIndex;
                }
                String head_line = "";
                if (headLineIndex != -1) {
                    head_line = doc_attr.substring(headLineIndex, doc_attr.indexOf("</HEADLINE>")).replace("<HEADLINE>", " ").trim();
                    System.out.println("HEADLINE: " + head_line);
                    docSplitIndex = headLineIndex;
                }

                String doc_text;
                if (docSplitIndex == -1) {
                    doc_text = doc_attr.trim().replace("<DOC", " ").trim();
                    int ind = doc_text.lastIndexOf(">");
                    doc_text = new StringBuilder(doc_text).replace(ind, ind + 1, "").toString().trim();
                } else {
                    doc_text = doc_attr.substring(0, docSplitIndex).trim().replace("<DOC", " ").trim();
                    int ind = doc_text.lastIndexOf(">");
                    doc_text = new StringBuilder(doc_text).replace(ind, ind + 1, "").toString().trim();
                }
                String[] attr = doc_text.split("\\s+|=");
                int i = 0;
                String doc_id = "";
                String doc_type = "";
                while (i < attr.length) {
                    if (attr[i].compareToIgnoreCase("ID") == 0) {
                        doc_id = attr[i + 1].replace("\"", "");
                        i = i + 2;
                    } else if (attr[i].compareToIgnoreCase("TYPE") == 0) {
                        doc_type = attr[i + 1].replace("\"", "");
                        i = i + 2;
                    } else {
                        i = i + 1;
                    }
                }
                System.out.println("DOC ID: " + doc_id + " TYPE: " + doc_type);
                if (doc_type.trim().compareToIgnoreCase("story") == 0) {
                    {
                        String[] text_list = trim_doc_str.substring(textIndex).replace("</DOC>", "")
                                .replace("<TEXT>", "").replace("</TEXT>", "")
                                .replace("<P>", "").split("</P>");
                        for (int lt = 0; lt < text_list.length; lt++) {
                            String x = text_list[lt];
                            String s = x.trim();
                            if (!s.isEmpty()) {
                                if (!s.endsWith(".")) {
                                    s = s + ".";
                                    text_list[lt] = s;
                                }
                            }
                        }
                        StringBuilder builder = new StringBuilder();
                        for (String str : text_list) {
                            builder.append(str);
                        }
                        String text_ = builder.toString();
                        System.out.println(text_);
                        DBObject result;
                        if (shiftReduceParsing) {
                            result = parseSentenceUsingShiftReduce(text_, doc_type, doc_id, head_line, date_line, parserModelPath,taggerPath);
                        } else {
                            result = parseSentenceUsingDefaultParser(text_, doc_type, doc_id, head_line, date_line, parserModelPath,taggerPath);
                        }
                        return result;
                    }
                }
            }
        }
        return null;
    }

    private DBObject parseSentenceUsingDefaultParser(String sentences, String doc_type, String doc_id, String headLine, String dateLine, String parserModelPath, String taggerPath) throws FileNotFoundException {
        BasicDBObject outterMongoDBObj = new BasicDBObject();
        outterMongoDBObj.put("type", doc_type);
        outterMongoDBObj.put("doc_id", doc_id);
        outterMongoDBObj.put("head_line", headLine);
        outterMongoDBObj.put("date_line", dateLine);


        String[] options = {"-maxLength", "140", "-MAX_ITEMS", "1000000"};
        LexicalizedParser lp = LexicalizedParser.loadModel(parserModelPath, options);

        String[] parameters = {
                "normArDigits", "normAlif", "normYa", "removeDiacritics", "removeTatweel", "removeProMarker",
                "removeSegMarker", "removeMorphMarker", "removeLengthening", "atbEscaping"
        };
        Properties tokenizerOptions = StringUtils.argsToProperties(parameters);
        TokenizerFactory tokenizerFactory = ArabicTokenizer.factory();

        if (tokenizerOptions.containsKey("atb")) {
            tokenizerFactory = ArabicTokenizer.atbFactory();
        }

        for (String option : parameters) {
            tokenizerFactory.setOptions(option);
        }
        tokenizerFactory.setOptions("tokenizeNLs");
        Properties Prop = new Properties();
        Prop.setProperty(dateLine, dateLine);

        StringTokenizer st = new StringTokenizer(sentences, ".");
        int sentence_id = 1;
        boolean closeSentenceflag = false;

        ArrayList<BasicDBObject> innerDbList = new ArrayList<BasicDBObject>();

        while (st.hasMoreElements()) {
            String sentc = st.nextToken();
            sentc = sentc.replace(".", "").trim();
            if (!sentc.isEmpty()) {
                BasicDBObject innerMongoDBObj = new BasicDBObject();
                Tokenizer<CoreLabel> tok = tokenizerFactory.getTokenizer(new StringReader(sentc));
                List<CoreLabel> rawWords = tok.tokenize();
                Tree parse = lp.apply(rawWords);
                innerMongoDBObj.put("sentence_id", sentence_id);
                innerMongoDBObj.put("sentence", sentc);
                innerMongoDBObj.put("parse_sentence", parse.toString());
                innerMongoDBObj.put("token", rawWords.toString());
                innerDbList.add(innerMongoDBObj);
                sentence_id++;
            }

        }
        outterMongoDBObj.put("sentences", innerDbList);

        return outterMongoDBObj;
    }

    private DBObject parseSentenceUsingShiftReduce(String sentences, String doc_type, String doc_id, String headLine, String dateLine, String parserModelPath, String taggerPath) throws FileNotFoundException {

        BasicDBObject outterMongoDBObj = new BasicDBObject();
        outterMongoDBObj.put("type", doc_type);
        outterMongoDBObj.put("doc_id", doc_id);
        outterMongoDBObj.put("head_line", headLine);
        outterMongoDBObj.put("date_line", dateLine);


//        String[] options = {"-maxLength", "140", "-MAX_ITEMS", "500000"};
        MaxentTagger tagger = new MaxentTagger(taggerPath);
        ShiftReduceParser srp = ShiftReduceParser.loadModel(parserModelPath);

        String[] parameters = {
                "normArDigits", "normAlif", "normYa", "removeDiacritics", "removeTatweel", "removeProMarker",
                "removeSegMarker", "removeMorphMarker", "removeLengthening", "atbEscaping"
        };
        Properties tokenizerOptions = StringUtils.argsToProperties(parameters);
        TokenizerFactory tokenizerFactory = ArabicTokenizer.factory();

        if (tokenizerOptions.containsKey("atb")) {
            tokenizerFactory = ArabicTokenizer.atbFactory();
        }

        for (String option : parameters) {
            tokenizerFactory.setOptions(option);
        }
        tokenizerFactory.setOptions("tokenizeNLs");
        Properties Prop = new Properties();
        Prop.setProperty(dateLine, dateLine);

        StringTokenizer st = new StringTokenizer(sentences, ".");
        int sentence_id = 1;

        ArrayList<BasicDBObject> innerDbList = new ArrayList<BasicDBObject>();
        while (st.hasMoreElements()) {
            String sentc = st.nextToken();
            sentc = sentc.replace(".", "").trim();
            if (!sentc.isEmpty()) {
                BasicDBObject innerMongoDBObj = new BasicDBObject();
                Tokenizer<HasWord> tok = tokenizerFactory.getTokenizer(new StringReader(sentc));
                List<HasWord> rawWords = tok.tokenize();
                List<TaggedWord> tagged = tagger.tagSentence(rawWords);
                Tree parse = srp.apply(tagged);

                innerMongoDBObj.put("sentence_id", sentence_id);
                innerMongoDBObj.put("sentence", sentc);
                innerMongoDBObj.put("parse_sentence", parse.toString());
                innerMongoDBObj.put("token", rawWords.toString());

                sentence_id++;

                innerDbList.add(innerMongoDBObj);
            }


        }
        outterMongoDBObj.put("sentences", innerDbList);
        return outterMongoDBObj;
    }
}