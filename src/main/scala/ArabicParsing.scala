

/**
 * Created by root on 11/30/15.
 */

object ArabicParsing {

  def main (args: Array[String]): Unit = {

    var start_time = System.currentTimeMillis()


    val  xml_String = "<DOC id=\"XIN_ARB_20080901.0001\" type=\"story\" >  <HEADLINE>  اخبار عالمية قصيرة ومنوعة  </HEADLINE>  <TEXT>  <P>  اخبار عالمية قصيرة ومنوعة  </P>  <P>  بكين اول سبتمبر 2008 (شينخوا) فيما يلى مجموعة من الاخبار العالمية  القصيرة والمنوعة:  </P>  <P>    </P>  <P>  لاغوس -- قالت السلطات يوم الاحد إن شرطة مكافحة الكسب غير المشروع  ضبطت نحو 630 ألف دولار جمعت خلال حفل عشاء نظمته جماعة \"افارقة من  اجل اوباما\" لتأييد حملة باراك اوباما المرشح لانتخابات الرئاسة  الامريكية.  </P>  <P>  وفي حين أن جمع التبرعات غير مخالف للقانون في نيجيريا الا أن  القانون الامريكي يحظر على جماعات الحملات الاجنبية تقديم أموال  للاحزاب السياسية. وقالت حملة اوباما انها غير مرتبطة اطلاقا بجماعة  \"أفارقة من اجل اوباما\" ولن تقبل أموالا منها.  </P>  <P>  وأثار المبلغ الضخم الذي جمع خلال العشاء غضبا شعبيا على نطاق واسع  في أكبر دولة منتجة للنفط في افريقيا حيث يعيش كثير من السكان على  أقل من دولارين في اليوم ودفع وكالة مكافحة الجرائم المالية  والاقتصادية لبدء تحقيق.  </P>  <P>  موسكو -- أشادت وسائل الاعلام الروسية يوم الاحد برئيس الوزراء  فلاديمير بوتين لانقاذه طاقما تلفزيونيا من هجوم نمر سيبيري في  أحراش بأقصى الشرق.  </P>  <P>  ونجح بوتين خلال استراحة توقف خلالها عن التنديد بالغرب بخصوص  جورجيا في انقاذ الطاقم فيما يبدو خلال رحلة الى متنزه وطني للاطلاع  على كيفية مراقبة الباحثين للنمور في الحياة البرية.  </P>  <P>  وقالت محطة التلفزيون الرئيسية في البلاد إنه لدى وصول بوتين مع  مجموعة من خبراء الحياة البرية لمشاهدة نمر محاصر من نوع امور فر  الحيوان وركض في اتجاه طاقم تصوير قريب منه. وسارع بوتين بتخدير  النمر باطلاق قذيفة مخدرة عليه من بندقية خاصة. (يتبع)  </P>  </TEXT>  </DOC>";

    val parser = new ArabicParserForMongoDB()

    var result = parser.parseArabicDoc(xml_String, false,
      "/opt/custom_lib/models/arabicFactored.ser.gz","/opt/TestSpark/custom_lib/tagger/arabic.tagger");

    println(result.toString)

    var timeInSecond = (System.currentTimeMillis() - start_time)
    println("Total time (millisec) to process a document (Without Shift reduce parser): " + timeInSecond + "\n")


    start_time = System.currentTimeMillis()

    result = parser.parseArabicDoc(xml_String, true,
      "/opt/custom_lib/models/arabicSR.ser.gz","/opt/TestSpark/custom_lib/tagger/arabic.tagger");

    println(result.toString)

    timeInSecond = (System.currentTimeMillis() - start_time)
    println("Total time (millisec) to process a document (Shift reduce parser): " + timeInSecond + "\n")



  }
}
