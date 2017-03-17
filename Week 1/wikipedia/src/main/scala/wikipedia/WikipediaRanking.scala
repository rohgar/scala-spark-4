package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String)

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  // Set up the Spark Context
  val conf: SparkConf = new SparkConf
  val sc: SparkContext = new SparkContext(master = "local[*]", appName = "wikipedia", conf)
  sc.setLogLevel("ERROR")

  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val stringRDD = sc.textFile(WikipediaData.filePath) // This is a collections of Strings
  val wikiRdd: RDD[WikipediaArticle] = stringRDD.map(arg => WikipediaData.parse(arg)) // This is a collections of Wikipedia articles

  /**
   * Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: should you count the "Java" language when you see "JavaScript"?
   *  Hint3: the only whitespaces are blanks " "
   *  Hint4: no need to search in the title :)
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.filter(
        article => article.text.split(" ").contains(lang)
    ).count().intValue()
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs.map(lang => (lang -> occurrencesOfLang(lang, rdd))).sortBy(_._2).reverse
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    // 1. find out all languages that are contained in an article: we get List[lang] that are contained in the article
    // 2. create a tuple of each of this filtered-lang and the article
    // 3. For the whole collection, now we have a flat List[(filteredLang, article)], so use groupByKey to group using lang to give RDD[(String, List[WikipediaArticle])] 
    rdd.flatMap(
      (article) => (
        langs.filter(lang => article.text.split(" ").contains(lang)) // List[lang]
        ).map(filteredLang => (filteredLang -> article))                                    // List[(filteredLang, article)]
      ).groupByKey()                                                                        // RDD[(String, List[WikipediaArticle])]       
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.map( elem => (elem._1, elem._2.size) ).collect().toList.sortBy(_._2).reverse
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd.flatMap(
      (article) => (
        langs.filter(lang => article.text.split(" ").contains(lang))                        // List[lang]
        ).map(filteredLang => (filteredLang, 1))                                            // List[(filteredLang, 1)]
        ).reduceByKey( _ + _ )                                                              // RDD[(lang, totalcntinwiki)]
        .collect()                                                                          // List[(lang, totalcntinwiki)]
        .toList.sortBy(_._2).reverse                                                        // List[(lang, totalcntinwiki)]
  }

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
