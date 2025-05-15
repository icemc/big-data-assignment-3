import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import scala.collection.mutable._
import scala.io.Source
import java.util.regex.Pattern
import scala.collection.mutable

/**
 * RunLSA_SimpleTokenizer: Latent Semantic Analysis of Wikipedia Articles using Regular Expression Tokenization
 *
 * This implementation uses simple regex tokenization instead of the NLP pipeline.
 */
object RunLSASimpleTokenizer2 {

  def main(args: Array[String]): Unit = {
    // Set up logging level to reduce verbosity
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Initialize Spark
    val spark = SparkSession
      .builder()
      .appName("RunLSA_SimpleTokenizer")
      .getOrCreate()

    val sc = spark.sparkContext

    // Parse and process the Wikipedia Articles
    processWikipediaArticles(sc)

    // Clean up
    spark.stop()
  }

  /**
   * Parse the header of a Wikipedia article document
   */
  def parseHeader(line: String): Array[String] = {
    try {
      var s = line.substring(line.indexOf("id=\"") + 4)
      val id = s.substring(0, s.indexOf("\""))
      s = s.substring(s.indexOf("url=\"") + 5)
      val url = s.substring(0, s.indexOf("\""))
      s = s.substring(s.indexOf("title=\"") + 7)
      val title = s.substring(0, s.indexOf("\""))
      Array(id, url, title)
    } catch {
      case e: Exception => Array("", "", "")
    }
  }

  /**
   * Parse the lines of a Wikipedia article into (title, content) pairs
   */
  def parse(lines: Array[String]): Array[(String, String)] = {
    val docs = ArrayBuffer.empty[(String, String)]
    var title = ""
    var content = ""
    for (line <- lines) {
      try {
        if (line.startsWith("<doc ")) {
          title = parseHeader(line)(2)
          content = ""
        } else if (line.startsWith("</doc>")) {
          if (title.nonEmpty && content.nonEmpty) {
            docs += ((title, content))
          }
        } else {
          content += line + "\n"
        }
      } catch {
        case e: Exception => content = ""
      }
    }
    docs.toArray
  }

  // Define regex patterns for tokenization
  val WORD_PATTERN: Pattern = Pattern.compile("^[a-z\\-]{6,24}$")
  val NUMBER_PATTERN: Pattern = Pattern.compile("^-?[0-9]+([.,][0-9]+)?$")

  /**
   * Tokenize text using simple regex patterns
   */
  def simpleTokenize(text: String, stopWords: mutable.Set[String]): mutable.Seq[String] = {
    val tokens = text.toLowerCase()
      .replaceAll("[^a-zA-Z0-9\\s\\-]", " ")
      .split("\\s+")
      .filter(token =>
        token.length > 2 &&
          !stopWords.contains(token) &&
          (WORD_PATTERN.matcher(token).matches() || NUMBER_PATTERN.matcher(token).matches())
      )
    tokens
  }

  /**
   * Process the Wikipedia articles and compute LSA
   */
  def processWikipediaArticles(sc: SparkContext): Unit = {
    println("Starting LSA processing with simple tokenizer...")

    // Parameters
    val sampleSize = 0.01  // Use all files in the sample
    val numTerms = 5000   // Number of most frequent terms to consider
    val k = 25            // Number of latent concepts

    // Load stop words
    val stopWords = sc.broadcast(
      Source.fromFile("../../input/wikipedia/stopwords.txt").getLines().toSet
    )

    // Read and parse the Wikipedia articles
    val textFiles = sc.wholeTextFiles("../../input/wikipedia/articles/*/*").sample(withReplacement = false, sampleSize)
    val numFiles = textFiles.count()
    println(s"Processing $numFiles files")

    val plainText = textFiles.flatMap { case (uri, text) => parse(text.split("\n")) }
    val numDocs = plainText.count()
    println(s"Found $numDocs documents")

    val bNumDocs = sc.broadcast(numDocs)

    // Process text with simple tokenizer
    val tokenized: RDD[(String, mutable.Seq[String])] = plainText.map {
      case (title, contents) => (title, simpleTokenize(contents, mutable.Set() ++ stopWords.value))
    }

    // Calculate term frequencies per document
    val docTermFreqs = tokenized.map {
      case (title, terms) => {
        val termFreqs = terms.foldLeft(new mutable.HashMap[String, Int]()) {
          (map, term) => {
            map += term -> (map.getOrElse(term, 0) + 1)
            map
          }
        }
        (title, termFreqs)
      }
    }
    docTermFreqs.cache()
    val docCount = docTermFreqs.count()
    println(s"Calculated term frequencies for $docCount documents")

    // Map document titles to IDs
    val docIds = docTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()

    // Calculate document frequencies
    val docFreqs = docTermFreqs.flatMap(_._2.keySet).map((_, 1)).reduceByKey(_ + _, 24)

    // Select the top terms by document frequency
    val ordering = Ordering.by[(String, Int), Int](_._2)
    val topDocFreqs = docFreqs.top(numTerms)(ordering)

    // Calculate inverse document frequencies
    val idfs = topDocFreqs.map {
      case (term, count) =>
        (term, math.log(bNumDocs.value.toDouble / count))
    }.toMap

    // Map terms to IDs
    val idTerms: Predef.Map[String, Int] = idfs.keys.zipWithIndex.toMap
    val termIds = idTerms.map(_.swap)

    val bIdfs = sc.broadcast(idfs)
    val bIdTerms = sc.broadcast(idTerms)

    // Create term vectors for each document
    val vecs = docTermFreqs.map(_._2).map(termFreqs => {
      val docTotalTerms = termFreqs.values.sum
      val termScores = termFreqs.filter {
        case (term, _) => bIdTerms.value.contains(term)
      }.map {
        case (term, _) => (bIdTerms.value(term), bIdfs.value(term) * termFreqs(term) / docTotalTerms)
      }.toSeq
      Vectors.sparse(bIdTerms.value.size, termScores)
    })

    vecs.cache()
    val vecCount = vecs.count()
    println(s"Created $vecCount term vectors")

    // Compute SVD
    val mat = new RowMatrix(vecs)
    println("Computing SVD...")
    val svd = mat.computeSVD(k, computeU = true)
    println("SVD computation completed")

    // Analyze and display the results
    val topConceptTerms = topTermsInTopConcepts(svd, k, 25, mutable.Map() ++ bIdTerms.value) //Dirty hack to convert immutable.map to mutable.map
    val topConceptDocs = topDocsInTopConcepts(svd, k, 25, docIds)

    // Print top terms and documents for each concept
    for (i <- 0 until k) {
      println(s"Concept $i:")
      println("  Top Terms: " + topConceptTerms(i).map(_._1).mkString(", "))
      println("  Top Docs: " + topConceptDocs(i).map(_._1).mkString(", "))
      println()
    }
  }

  /**
   * Find the top terms for each concept
   */
  def topTermsInTopConcepts(
                             svd: SingularValueDecomposition[RowMatrix, Matrix],
                             numConcepts: Int,
                             numTerms: Int,
                             idTerms: mutable.Map[String, Int]): mutable.Seq[mutable.Seq[(String, Double)]] = {

    val v = svd.V
    val topTerms = new ArrayBuffer[mutable.Seq[(String, Double)]]()
    val arr = v.toArray

    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map {
        case (score, id) =>
          (idTerms.find(_._2 == id).getOrElse(("", -1))._1, score)
      }
    }
    topTerms
  }

  /**
   * Find the top documents for each concept
   */
  def topDocsInTopConcepts(
                            svd: SingularValueDecomposition[RowMatrix, Matrix],
                            numConcepts: Int,
                            numDocs: Int,
                            docIds: scala.collection.Map[Long, String]): mutable.Seq[mutable.Seq[(String, Double)]] = {

    val u = svd.U
    val topDocs = new ArrayBuffer[mutable.Seq[(String, Double)]]()

    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
      topDocs += docWeights.top(numDocs).map {
        case (score, id) => (docIds(id), score)
      }
    }
    topDocs
  }
}