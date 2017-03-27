package stackoverflow

import org.scalatest.{ FunSuite, BeforeAndAfterAll }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  val testConf = new SparkConf().setMaster("local").setAppName("StackOverflowTest")
  val sc: SparkContext = new SparkContext(testConf)

  test("Grouping questions and answers together") {
    val postings = List(
      Posting(1, 1, None, None, 0, None),
      Posting(1, 2, None, None, 0, None),
      Posting(2, 3, None, Some(1), 2, None),
      Posting(2, 4, None, Some(1), 5, None),
      Posting(2, 5, None, Some(2), 12, None),
      Posting(1, 6, None, None, 0, None))
    val rdd = sc.makeRDD(postings)
    val results = testObject.groupedPostings(rdd).collect()

    assert(results.size === 2)
    assert(results.contains(
      (1, Iterable(
        (Posting(1, 1, None, None, 0, None), Posting(2, 3, None, Some(1), 2, None)),
        (Posting(1, 1, None, None, 0, None), Posting(2, 4, None, Some(1), 5, None))))))
    assert(results.contains(
      (2, Iterable(
        (Posting(1, 2, None, None, 0, None), Posting(2, 5, None, Some(2), 12, None))))))
  }

  test("Maximum scores among answers per question") {
    val groupedQuestions = Seq(
      (1, Iterable(
        (Posting(1, 1, None, None, 0, None), Posting(2, 3, None, Some(1), 2, None)),
        (Posting(1, 1, None, None, 0, None), Posting(2, 4, None, Some(1), 5, None)))),
      (2, Iterable(
        (Posting(1, 2, None, None, 0, None), Posting(2, 5, None, Some(2), 12, None)))),
      (3, Iterable(
        (Posting(1, 6, None, None, 0, None), Posting(2, 7, None, Some(3), 2, None)),
        (Posting(1, 6, None, None, 0, None), Posting(2, 8, None, Some(3), 19, None)),
        (Posting(1, 6, None, None, 0, None), Posting(2, 9, None, Some(3), 10, None)))))
    val rdd = sc.makeRDD(groupedQuestions)
    val results = testObject.scoredPostings(rdd).collect()

    assert(results.size == 3)
    assert(results.contains((Posting(1, 1, None, None, 0, None), 5)))
    assert(results.contains((Posting(1, 2, None, None, 0, None), 12)))
    assert(results.contains((Posting(1, 6, None, None, 0, None), 19)))
  }

  test("Vectors for clustering") {
    val questionsWithTopAnswer = List(
      (Posting(1, 1, None, None, 0, Some("Java")), 14),
      (Posting(1, 1, None, None, 0, None), 5),
      (Posting(1, 1, None, None, 0, Some("Scala")), 25),
      (Posting(1, 1, None, None, 0, Some("JavaScript")), 3))

    val rdd = sc.makeRDD(questionsWithTopAnswer)

    val result = testObject.vectorPostings(rdd).collect()
    assert(result === Array((50000, 14), (500000, 25), (0, 3)))
  }

  //Use this test only when you want to check how fast your implementations are
  test("Speed test of Grouped Postings + Scored Postings") {
    val lines = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw = testObject.rawPostings(lines)
    val grouped = testObject.groupedPostings(raw)
    val scored = testObject.scoredPostings(grouped)

    assert(grouped.collect().size === 2121822)

    scored.collect().take(10).foreach(println)
  }

}
